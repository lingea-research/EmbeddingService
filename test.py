
import embeddingService
import model
import timeit

def test1():
    def substest():
        es = embeddingService.EmbeddingService()
        e1 = es.get_embeddings(document="Test sentence.", model_name="sentence-transformers/distiluse-base-multilingual-cased-v2")
        e2 = es.get_embeddings(document="Test sentence 2.", model_name="sentence-transformers/distiluse-base-multilingual-cased-v2")
        e3 = es.get_embeddings(document="Test sentence 3.", model_name="sentence-transformers/distiluse-base-multilingual-cased-v2")
        return [e1, e2, e3]

    first = substest()
    second = substest()

    for i in range(len(first)):
        print("Testing embedding", i, end=" : ")
        if len(first[i]) != len(second[i]):
            print("WRONG LENGTH")
            break
        result = True
        for j in range(len(first[i])):
            result = result and (first[i][j] == second[i][j])
        print(result)

def test2():
    es = embeddingService.EmbeddingService()

    for i in range(1000):
        es.get_embeddings(document="Test sentence " + str(i), model_name="sentence-transformers/distiluse-base-multilingual-cased-v2")

def main():
    test1()
    # test2()

main()
