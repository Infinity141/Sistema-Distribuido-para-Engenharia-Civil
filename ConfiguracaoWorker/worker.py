import redis
from rq import Worker, Queue

#Defina o ip e a porta do computador do redis

REDIS_URL = 'redis://:liama@10.10.10.10:6379'
REDIS_IP ='10.10.10.10'

def iniciar():
	try:
		#Conectar ao Redis
		conexao_redis = redis.from_url(REDIS_URL)
		conexao_redis.ping()
		print("Conexao com o redis estabelecida")
		#Entra dentro da fila onde as funçoes serao enviadas
		worker = Worker(['operacoes'], connection=conexao_redis, serializer=cloudpickle)
		print("[*] Worker online e pronto para trabalhar..")
		#loop onde o worker ira infinitamente vigiar o redis
		worker.work()
	
	except Exception as e:
			print(f"Erro: {e}")


if __name__ == "__main__":
	iniciar()
