import redis
from rq import Worker, Queue, Connection

#Defina o ip e a porta do computador do redis
REDIS_IP = '10.10.10.10'
REDIS_PORT = 6379


def iniciar_worker():
	#Configura a conexão	
	try:
		conexao = redis.Redis(host=REDIS_IP, port=REDIS_PORT)
		
		#Conecta na fila
		with Connection(conexao):
			worker = Worker([operacoes])
			print(f"[*] Conectado ao Redis em {REDIS_IP}")
			print(f"[*] Aguardando ordens")
			
			#Começa os trabalhos
			#O computador entrará em loop e estará constantemente vendo se há trabalhos
			worker.work()
		except Exception as e:
			print(f"Erro ao conectar: {e}")

fi __name__ == "__main__":
	iniciar_worker()
