import redis
from rq.decorators import job
import cloudpickle

# Conecta no banco de dados redis
REDIS_URL = 'redis://:liama@10.10.10.10:6379'
conn = redis.from_url(REDIS_URL)

# Monta um mapa qual fila usar e qual serializador deve utilizar
# O timeout='1h' garante que cálculos longos não sejam interrompidos
tarefa_cluster = job('operacoes', connection=conn, serializer=cloudpickle, timeout=3600)
