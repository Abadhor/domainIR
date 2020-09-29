import argparse
import json
from wrappers.elastic import ESWrapper
from filereaders import QueryReader
from preprocessing import preprocess_document
import spacy

def post_json(data_in, url):
    data = json.dumps(data_in).encode('utf-8')
    req =  urllib.request.Request(url, data=data, headers={'content-type': 'application/json'}) # this will make the method "POST"
    resp = urllib.request.urlopen(req)
    resp_text = resp.read().decode('utf-8')
    return resp_text


def get_relevance_mask(ranking, relevant_docs):
    return [1 if doc_id in relevant_docs else 0 for doc_id,_score,normalized_score in ranking]
    
    
def get_rank_position(ranking, document_id_set):
    # For a set of documents, determine their positions in a ranking
    positions = {doc_id: None for doc_id in document_id_set}
    for idx, _id, _score, normalized_score in enumerate(ranking):
        if _id in positions:
            positions[_id] = idx
    return positions


def get_within_top_k(positions, top_k, is_irrelevant):
    ok_set = set()
    not_ok_set = set()
    for _id, p in positions.items():
        if is_ok(p, top_k, is_irrelevant):
            ok_set.add(_id)
        else:
            not_ok_set.add(_id)
    return ok_set, not_ok_set


def is_ok(p, top_k, is_irrelevant):
    if is_irrelevant:
        return p >= top_k
    else:
        return p < top_k


class Validator():
   
    
    def __init__(self, ir, nlp, delimiter):
        self.nlp = nlp
        self.ir = ir
        self.delimiter = delimiter
        self.EVALUATIONS_INV = {v:k for k,v in self.EVALUATIONS.items()}
        
    
    def validate(self, query_data, index_name, top_k):
        for query_id, query, relevant, irrelevant in query_data:
            query = " ".join(preprocess_document(self.nlp(query)))
            
            ranking = self.ir.retrieve_document_ranking(query, index_name)
            
            relevant_positions = get_rank_position(ranking, relevant)
            irrelevant_positions = get_rank_position(ranking, irrelevant)
            
            relevant_ok, relevant_not_ok = get_within_top_k(relevant_positions, top_k, irrelevant=False)
            irrelevant_ok, irrelevant_not_ok = get_within_top_k(irrelevant_positions, top_k, irrelevant=True)
            
            if len(relevant_not_ok) + len(irrelevant_not_ok) > 0:
                print("{query_id}\t{relevant_ok}\t{relevant_not_ok}-{irrelevant_not_ok}".format(
                    query_id,
                    relevant_ok,
                    relevant_not_ok,
                    irrelevant_not_ok
                ))
        
    

if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description=
    "Tests whether the correctness of search queries based on a list of expected results.")
    parser.add_argument("query_path", help="path to the file with the queries")
    parser.add_argument("index_name", help="name of the index to be searched")
    parser.add_argument("-d", "--delimiter", default="\t", help="delimiter used for the expected_results file")
    parser.add_argument("-k", "--top_k", type=int, default=10, help="evaluate documents up to rank k")
    args = parser.parse_args()
    
    nlp = spacy.load("en_core_web_sm")
    
    
    ir = ESWrapper(nlp)
    
    reader = QueryReader(delim=args.delimiter)
    query_data = reader(args.query_path, include_relevancy=True)
    
    val = Validator(ir, nlp, args.delimiter)
    val.validate(query_data, args.index_name, args.top_k)