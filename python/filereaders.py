
class QueryReader:
    
    def __init__(self, delim="\t"):
        self.delim = delim
    
    
    def __call__(self, query_path, include_relevancy=False):
        with open(query_path, "r") as fp:
            for line in fp:
                if include_relevancy == True:
                    line = line.strip()
                    query_id, query, relevancy = line.split(sep=self.delim)
                    relevant, irrelevant = self._get_relevant(relevancy)
                    yield (query_id, query, relevant, irrelevant)
                else:
                    query_id, query = line.split(sep=self.delim)
                    yield (query_id, query)
                    
    
    
    def _get_relevant(self, relevancy):
        relevancy = relevancy.split(sep="-")
        if len(relevancy) == 2:
            relevant, irrelevant = relevancy
            irrelevant = set(irrelevant.split(sep=','))
        else:
            relevant = relevancy[0]
            irrelevant = set()
        if relevant == '':
            relevant = set()
        else:
            relevant = set(relevant.split(sep=','))
        return relevant, irrelevant
        
        
class TRECReader:
    
    def __call__(self, judgement_path):
        mapping = {}
        with open(judgement_path, "r") as fp:
            for line in fp:
                query, _, doc_id, relevance = line.strip().split()
                if int(relevance) == 0:
                    continue
                if query not in mapping:
                    mapping[query] = set()
                mapping[query].add(doc_id)
        return mapping
    
    