import redis
from rq import Worker, Queue

#Defina o ip e a porta do computador do redis
#REDIS_IP = '10.10.10.10'
#REDIS_PORT = 6379
REDIS_URL = 'redis://:liama@10.10.10.10:6379'
REDIS_IP ='10.10.10.10'

#def iniciar_worker():
#	#Configura a conexão	
#	try:
#		conexao = redis.from_url(REDIS_URL)
#		
#		conexao.ping()
#		#Conecta na fila
#		with Connection(conexao):
#			worker = Worker(['operacoes'])
#			print(f"[*] Conectado ao Redis em {REDIS_IP}")
#			print(f"[*] Aguardando ordens")
#			#Começa os trabalhos
#			#O computador entrará em loop e estará constantemente vendo se há trabalhos
#			worker.work()
#	except redis.exceptions.AuthenticationError:
#		print("Erro: Senha do Redis incorreta.")
#	except Exception as e:
#			print(f"[ERRO]: {e}")

def iniciar():
	try:
		conexao_redis = redis.from_url(REDIS_URL)
		conexao_redis.ping()
		print("Conexao com o redis estabelecida")
		worker = Worker(['operacoes'], connection=conexao_redis, serializer=cloudpickle)
		print("[*] Worker online e pronto para trabalhar..")
		worker.work()
	
	except Exception as e:
			print(f"Erro: {e}")


if __name__ == "__main__":
	iniciar()
