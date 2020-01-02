import gensim
from gensim.test.utils import get_tmpfile
from gensim.models import KeyedVectors
import csv

with open('carsQueriesW2V.txt', 'r') as file:
    data = file.read().replace('\n', '')
sentences = [data.split()]
from gensim.models import word2vec
model = word2vec.Word2Vec(sentences, window=4,iter=1500, min_count=0, size=300, workers=4, sg=1)
model.train(sentences,total_examples=len(sentences),epochs=1000)
word_vectors = model.wv
print(word_vectors)
word_vectors.save("/Users/Petros/ptychiaki/SmartViews/vectors_cars.kv")

with open('/Users/Petros/ptychiaki/SmartViews/wv_embeddings_cars.tsv', 'w') as tsvfile:
    writer = csv.writer(tsvfile, delimiter='\t')
    words = word_vectors.vocab.keys()
    f = open("/Users/Petros/ptychiaki/SmartViews/car_labels.txt","w+")
    for i in words:
     f.write(i + '\n')
    f.close()
    for word in words:
        vector = word_vectors.get_vector(word).tolist()
        row = [word] + vector
        writer.writerow(row)
model.save('w2vModel_cars.model')
