from config_cluster import tarefa_cluster
import numpy as np

@tarefa_cluster
def processar_matriz_gigante(n):
    # O código que você quiser!
    data = np.random.rand(n, n)
    return np.linalg.inv(data) # Inversa de uma matriz

# --- PARA MANDAR PROS WORKERS ---
print("Enviando...")
# Em vez de chamar a função direto, você usa .delay()
meu_job = processar_matriz_gigante.delay(2000) 

print(f"O Worker está processando o Job {meu_job.id}")
