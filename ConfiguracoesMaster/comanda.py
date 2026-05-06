from config_cluster import tarefa_cluster
import numpy as np

@tarefa_cluster
def processar_matriz_gigante(n):
    # O codigo com a funcao que voce quer calcular
    data = np.random.rand(n, n)
    return np.linalg.inv(data) # Inversa de uma matriz

# --- MANDAR PROS WORKERS ---
print("Enviando...")
# Em vez de chamar a função direto, você usa .delay()
meu_job = processar_matriz_gigante.delay(2000) 

print(f"O Worker está processando o Job {meu_job.id}")
