# coding: utf-8

from pyspark import SparkConf, SparkContext
from operator import add

def count_hosts_unique(requestsRDD):
    try:    
       map_lines = requestsRDD.map(lambda x:x.split(" "))
       create_records = map_lines.map(lambda x:(x[0],1))
       count_hosts = create_records.reduceByKey(lambda x,y: x+y)
       count_hosts = count_hosts.filter(lambda x: x[1]==1).count()
    except Exception as error:
        print(error)
    return count_hosts
    
def total_errors_404(requestsRDD):
    code_error = '404'
    try: 
        map_lines = requestsRDD.map(lambda x:x.split(" "))
        count_errors = map_lines.map(lambda x:(x[8],1)).filter(lambda x:x[0]==code_error).count()
    except Exception as error:
        print(error)
    return count_errors

def urls_errors_404(requestsRDD):
    map_lines = requestsRDD.map(lambda line: line.split('"')[1].split(' ')[1])
    counts = map_lines.map(lambda endpoint: (endpoint, 1)).reduceByKey(add)
    urls_top = counts.sortBy(lambda pair: -pair[1]).take(5)
    
    print('Top 5 urls com errors 404:')
    for endpoint, count in urls_top:
        print(endpoint, count)
    return

def number_of_errors_day(requestsRDD):
    days_request = requestsRDD.map(lambda line: line.split('[')[1].split(':')[0])
    counts_request = days_request.map(lambda day: (day, 1)).reduceByKey(add).collect()
    
    print('Quantidade de erros por dia')
    for day, count in counts_request:
        print(day, count)
    return

def total_bytes(requestsRDD):
    def count_bt(line):
        try:
            count = int(line.split(" ")[-1])
            if count < 0:
                ValueError()
            return count
        except:
            return 0

    count = rdd.map(count_bt).reduce(add)
    return

if __name__ == "__main__":

    sc = SparkContext()

    requisicao_jul = sc.textFile("access_log_Jul95")
    requisicao_aug = sc.textFile("access_log_Aug95")
    
    requestsRDD = sc.union([requestsRDD_jul,requestsRDD_aug])

    print("Considerando as respostas a partir da união dos RDDs.")
    
    number_host = count_hosts_unique(requestsRDD)
    print("Questão 1")
    print("Número de hosts únicos: ".format(number_host))
    
    total_errors = total_errors_404(requestsRDD) 
    print("Questao 2")
    print("O total de erros 404: ".format(total_errors))
    
    print("Questao 3")
    urls_errors_404(requestsRDD) 
    
    print("Questao 4")
    number_of_errors_day(requestsRDD)
    
    total_bytes_requests  = total_bytes(requestsRDD) 
    print("Questao 5")
    print("Total de bytes retornados: ".format(total_bytes_requests))
