import redis
import cloudpickle
import io
import numpy as np
import time
import logging

# --- CONFIGURAÇÃO DO SISTEMA DE LOGS ---
logging.basicConfig(
    filename='worker_atividades.log',
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Coloque o IP real da sua VM Debian configurada em modo Bridge
REDIS_URL = 'redis://:liama@10.10.10.10:6379' 

print("🐝 Worker Dinâmico aguardando instruções... (Monitorando via logs)")

# 1. TESTE E REGISTRO DE CONEXÃO INICIAL
try:
    r = redis.from_url(REDIS_URL)
    if r.ping():
        msg_sucesso = "Conexão com o Broker Redis estabelecida com sucesso!"
        print(f"✅ {msg_sucesso}")
        logging.info(msg_sucesso)
except Exception as e:
    msg_erro = f"Falha crítica na conexão inicial com o Redis: {e}"
    print(f"❌ {msg_erro}")
    logging.error(msg_erro)
    exit(1)

NOME_FILA_TAREFAS = 'fila_calculos_matriz'
NOME_LISTA_SINAL_TRABALHO = 'fila_calculos_matriz_processando'
NOME_DA_LISTA_RESULTADOS = 'lista_resultados_matriz'

# 2. LOOP PERPÉTUO DE PROCESSAMENTO
while True:
    try:
        # Padrão Ack Seguro: Retira da fila principal e joga na de backup atomicamente
pacote_bytes = r.rpoplpush(NOME_FILA_TAREFAS, NOME_LISTA_SINAL_TRABALHO)

        if pacote_bytes:
            # Reconstrói os dados da tarefa enviados pelo Master
            tarefa = cloudpickle.loads(pacote_bytes)
            job_id = tarefa['job_id']

            print(f"\n📥 [{job_id}] Recebido! Processando...")
            logging.info(f"Iniciando o processamento do {job_id}")

            # Desserializa a FUNÇÃO INTEIRA vinda da rede
            funcao_dinamica = cloudpickle.loads(tarefa['funcao_bytes'])
            p, particoes = tarefa['args']

            # Executa a lógica matemática dinamicamente
            lista_resultados = funcao_dinamica(p, particoes)

            # Transforma a resposta em bytes para tráfego rápido no Redis
            buffer = io.BytesIO()
            np.save(buffer, np.array(lista_resultados))
            dados_bytes = buffer.getvalue()

            # Devolve o resultado final para o Master
            r.rpush(NOME_DA_LISTA_RESULTADOS, dados_bytes)

            # Remove o job da lista de backup, pois ele terminou com sucesso
            r.lrem(NOME_LISTA_SINAL_TRABALHO, 1, pacote_bytes)
            print(f"✅ [{job_id}] Concluído.")

            # Grava o sucesso no arquivo .log
            msg_retorno = f"O {job_id} foi finalizado com sucesso e o valor foi retornado ao Master."
            logging.info(msg_retorno)

        else:
            # Se a fila estiver vazia, dorme 1 segundo para não estressar a CPU
            time.sleep(1)

    except redis.exceptions.ConnectionError:
        msg_queda = "A conexão com o Redis caiu no meio do loop! Tentando reconectar em 5 segundos..."
        print(f"⚠️ {msg_queda}")
        logging.warning(msg_queda)
        time.sleep(5)

    except Exception as e:
        msg_falha = f"Erro inesperado ao processar tarefa: {e}"
        print(f"❌ {msg_falha}")
        logging.error(msg_falha)
        time.sleep(1)
