import redis
import cloudpickle
import io
import numpy as np
import time
import logging
import socket  # Biblioteca nativa para pegar o nome da máquina

# --- CONFIGURAÇÃO DO SISTEMA DE LOGS ---
logging.basicConfig(
    filename='worker_atividades.log',
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Coloque o IP real do computador Master que roda o Redis
REDIS_URL = 'redis://:liama@10.10.10.10:6379' 

print("🐝 Worker Dinâmico iniciado... (Acompanhe os detalhes via log)")

# 1. TESTE DE CONEXÃO INICIAL
try:
    r = redis.from_url(REDIS_URL)
    if r.ping():
        msg = "Conexão com o Broker Redis estabelecida com sucesso!"
        print(f"✅ {msg}")
        logging.info(msg)
except Exception as e:
    msg_erro = f"Falha crítica na conexão inicial com o Redis: {e}"
    print(f"❌ {msg_erro}")
    logging.error(msg_erro)
    exit(1)

NOME_FILA_TAREFAS = 'fila_calculos'
NOME_LISTA_SINAL_TRABALHO = 'fila_calculos_processando'
NOME_DA_LISTA_RESULTADOS = 'lista_resultados'

# 2. LOOP DE PROCESSAMENTO SEGURO
while True:
    try:
        # Pega a tarefa da fila principal e joga na de backup (Ack Seguro)
        pacote_bytes = r.rpoplpush(NOME_FILA_TAREFAS, NOME_LISTA_SINAL_TRABALHO)
        
        if pacote_bytes:
            # Reconstrói os dados enviados pelo Master
            tarefa = cloudpickle.loads(pacote_bytes)
            job_id = tarefa['job_id']
            
            print(f"\n📥 [{job_id}] Recebido! Iniciando processamento...")
            logging.info(f"Iniciando o processamento do {job_id}")
            
            # Desserializa a função real vinda da rede
            funcao_dinamica = cloudpickle.loads(tarefa['funcao_bytes'])
            p, particoes = tarefa['args']
            
            # --- EXECUTANDO A FUNÇÃO DINÂMICA ---
            lista_resultados = funcao_dinamica(p, particoes)
            
            # Transforma a resposta matemática em bytes
            buffer = io.BytesIO()
            np.save(buffer, np.array(lista_resultados))
            dados_bytes = buffer.getvalue()
            
            # Captura o nome real deste computador físico na rede
            nome_desta_maquina = socket.gethostname()
            
            # Monta o dicionário estruturado com o identificador
            pacote_retorno = {
                'worker_id': nome_desta_maquina,
                'job_id': job_id,
                'resultado_bytes': dados_bytes
            }
            pacote_retorno_bytes = cloudpickle.dumps(pacote_retorno)
            
            # Envia o pacote completo com o ID da máquina para o Master
            r.rpush(NOME_DA_LISTA_RESULTADOS, pacote_retorno_bytes)
            
            # ATENÇÃO AQUI: Remove da fila de backup usando o pacote original recebido
            r.lrem(NOME_LISTA_SINAL_TRABALHO, 1, pacote_bytes)
            
            print(f"✅ [{job_id}] Concluído e enviado pelo host [{nome_desta_maquina}].")
            logging.info(f"O {job_id} foi finalizado com sucesso por [{nome_desta_maquina}] e retornado.")
            
        else:
            time.sleep(1)
            
    except redis.exceptions.ConnectionError:
        msg_queda = "A conexão com o Redis caiu! Tentando reconectar em 5 segundos..."
        print(f"⚠️ {msg_queda}")
        logging.warning(msg_queda)
        time.sleep(5)
        
    except Exception as e:
        msg_falha = f"Erro inesperado ao processar tarefa: {e}"
        print(f"❌ {msg_falha}")
        logging.error(msg_falha)
        
        # Se falhar no meio do cálculo, devolve para a fila principal e tira do backup
        try:
            if 'pacote_bytes' in locals() and pacote_bytes:
                r.rpush(NOME_FILA_TAREFAS, pacote_bytes)
                r.lrem(NOME_LISTA_SINAL_TRABALHO, 1, pacote_bytes)
        except:
            pass
        time.sleep(1)
