import sys
import json
import gensim

def compute_similarity(current_query,victims,model):
    similarity_scores=[]
    for i in range(len(victims)):
        score = model.wv.similarity(current_query,victims[i])
        similarity_scores.append(score)
    return similarity_scores

model = gensim.models.Word2Vec.load("w2vModel.model")
data = (json.dumps(sys.argv[1]))
current=sys.argv[2]
data = data.replace('[','')
data = data.replace(']','')
data = data.replace('"','')
victims = data.split(',')
similarity_scores = compute_similarity(current,victims,model)
#JSON Array with objects formated like {viewName: "AB", simScore: 0.235655}
print(similarity_scores)

