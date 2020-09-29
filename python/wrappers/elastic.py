import json
import os
from elasticsearch import Elasticsearch
from elasticsearch.client import IndicesClient, CatClient
from tqdm import tqdm
import re

from preprocessing import preprocess_document

class ESWrapper:
    
    def __init__(self, nlp):
        self.es = Elasticsearch()
        self.ic = IndicesClient(self.es)
        self.cat = CatClient(self.es)
        self.nlp = nlp
    
    
    def init_index(index_name, data_path, template):
        index = ESIndex(index_name, data_path, template, self.es, self.ic, self.nlp)
        return index
    
    
    def analyze_query(self, preprocessed_query, index):
        body = {
            "field": "contents",
            "text": preprocessed_query
        }
        response = self.ic.analyze(body=body, index=index.index_name)
        tokens = [t['token'] for t in response['tokens']]
        return tokens
    
    
    def generate_query(self, query):
        return {
            "match": {
                "contents": query
            }
        }
    
    
    def retrieve_document_ranking(self, query, index_name, page_size=100, all_results=True):
        retrieved_document_count = None
        ranking = []
        from_doc = 0
        while retrieved_document_count != 0:
            body = {
                "from": from_doc,
                "size": page_size,
                "query": self.generate_query(query)
            }
            response = self.es.search(index=index_name, body=body)
            # scores are within interval [0, max_score]
            max_score = response['hits']['max_score']
            
            retrieved_documents = []
            for hit in response["hits"]["hits"]:
                _id = hit["_id"]
                _score = hit["_score"]
                normalized_score = _score/max_score
                retrieved_documents.append((_id, _score, normalized_score))
            retrieved_document_count = len(retrieved_documents)
            ranking.extend(retrieved_documents)
            from_doc += retrieved_document_count
            if not all_results:
                break
        return ranking
    
    
        

class ESIndex:
    
    def __init__(self, index_name, data_path, template, es, ic, nlp):
        self.es = es
        self.ic = ic
        self.nlp = nlp
        self.index_name = index_name
        self.data_path = data_path
        self.template = template
    
    
    def create_index(self, params):
        with open(self.template, "r") as fp:
            raw_body = fp.read()
        # fill template
        index_body = json.loads(raw_body % params)
        if self.ic.exists(self.index_name):
            self.ic.delete(self.index_name)
        self.ic.create(self.index_name, body=index_body)
    
    
    def delete_index(self):
        if self.ic.exists(self.index_name):
            self.ic.delete(self.index_name)
        
    
    def update_settings(self, params):
        with open(self.template, "r") as fp:
            raw_body = fp.read()
        # fill template
        index_body = json.loads(raw_body % params)
        settings = index_body['settings']
        del settings["index"]["number_of_shards"]
        del settings["index"]["number_of_replicas"]
        self.ic.close(self.index_name)
        self.ic.put_settings(settings, self.index_name)
        self.ic.open(self.index_name)
        
    
    def index_exists(self):
        return self.ic.exists(self.index_name)
        
    
    def index_docs(self):
        documents = []
        doc_ids = []
        for fname in tqdm(os.listdir(self.data_path)):
            doc_id = fname[:len(fname)-len(".txt")]
            with open(os.path.join(self.data_path, fname), "r") as fp:
                document = fp.read()
                documents.append(document)
                doc_ids.append(doc_id)
        for doc_id, doc in tqdm(zip(doc_ids, self.nlp.pipe(documents, batch_size=64))):
            tokens = preprocess_document(doc)
            doc = {
                'contents': " ".join(tokens)
            }
            res = self.es.index(index=self.index_name, body=doc, id=doc_id)
            #res = self.es.index(index=self.index_name, body=doc)
        

class ESExplainer:
    
    def __init__(self, es):
        self.es = es
        prefix = r'(?P<prefix>weight\(contents:)'
        suffix = r'(?P<suffix> in \d+\))'
        self.exp_token_pattern = re.compile(prefix + r'(?P<token>.+?)' + suffix)
        
        
        prefix = r'(?P<prefix>score\(freq=)'
        suffix = r'(?P<suffix>\))'
        self.exp_freq_pattern = re.compile(prefix + r'(?P<freq>.+?)' + suffix)
    
    
    def get_document_explanation(self, document_id, query, index_name):
        body = {
            "query": query
        }
        token_dict = {}
        try:
            response = self.es.explain(index_name, document_id, body=body)
        except:
            return token_dict
        explained_tokens = response['explanation']['details']
        for explained_token in explained_tokens:
            token, val, freq = self.extract_explained_token_data(explained_token)
            token_dict[token] = {"val":val, "freq":freq}
        return token_dict
    
    
    def extract_explained_token_data(explained_token):
        match = self.exp_token_pattern.match(explained_token['description'])
        token = match.group('token')
        val = explained_token['value']
        match = self.exp_freq_pattern.match(explained_token['details'][0]['description'])
        freq = match.group('freq')
        return token, val, freq
