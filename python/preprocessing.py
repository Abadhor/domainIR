import re

def preprocess_document(doc, additional_stopwords=None):
    if additional_stopwords == None:
        additional_stopwords = {}
    return [t.lemma_.lower() for t in doc if is_accepted_token(t, additional_stopwords)]

    
def is_accepted_token(token, additional_stopwords):
    return (not token.is_stop and not token.pos_ == 'PUNCT' and not is_number(token) and not token.text in additional_stopwords and not token.lemma_.lower().strip() == "")


numeric = r'[\d.,\-+:/()%§]*'
number_pattern = re.compile(numeric+r'\d+'+numeric)
def is_number(token):
    if number_pattern.match(token.text) != None:
        return True
    else:
        return False
    
    
raw_query_pattern = re.compile(r'^(?P<query_id>.+?)(?P<delim>\|\|)(?P<query_content>.*)$')
def extract_raw_query(raw_query_line):
    line = raw_query_line.strip()
    m = raw_query_pattern.match(line)
    assert m != None
    query_id = m.group("query_id")
    query = m.group("query_content")
    return query_id, query


space = re.compile(r'[\-,.]')
empty = re.compile(r'[^A-Za-z_\s]')
def aggressive_preprocess(text):
    text = re.sub(space, " ", text)
    text = re.sub(empty, "", text)
    return text.lower().strip().split()


prefix = r'(?P<prefix>weight\(contents:)'
suffix = r'(?P<suffix> in \d+\))'
exp_token_pattern = re.compile(prefix + r'(?P<token>.+?)' + suffix)
"""
match = exp_token_pattern.match('weight(contents:12 in 2876) '
                                             '[PerFieldSimilarity], result of:')
match.group("token")
"""

prefix = r'(?P<prefix>score\(freq=)'
suffix = r'(?P<suffix>\))'
exp_freq_pattern = re.compile(prefix + r'(?P<freq>.+?)' + suffix)
"""
match = exp_freq_pattern.match('score(freq=1.0), '
                                                          'computed as boost * '
                                                          'idf * tf from:')
match.group("freq")
"""
def extract_explained_token_data(explained_token):
    match = exp_token_pattern.match(explained_token['description'])
    token = match.group('token')
    val = explained_token['value']
    match = exp_freq_pattern.match(explained_token['details'][0]['description'])
    freq = match.group('freq')
    return token, val, freq


def get_relevancy_mapping(ground_truth_path):
    # Returns all relevant documents per query
    mapping = {}
    with open(ground_truth_path, "r") as fp:
        for line in fp:
            query, _, doc_id, relevance = line.strip().split()
            if int(relevance) == 0:
                continue
            if query not in mapping:
                mapping[query] = set()
            mapping[query].add(doc_id)
    return mapping
