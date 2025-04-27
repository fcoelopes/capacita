import os
import time
import pandas as pd
from docx import Document
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Caminhos
raw_folder = './data/raw'
preprocessed_folder = './data/preprocessed'
processed_folder = './data/processed'

os.makedirs(raw_folder, exist_ok=True)
os.makedirs(preprocessed_folder, exist_ok=True)
os.makedirs(processed_folder, exist_ok=True)

class WordFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.docx'):
            time.sleep(1)  # Pequeno atraso para garantir que o arquivo foi completamente escrito
            processar_documento(event.src_path)

def extrair_tabelas_com_python_docx(caminho):
    doc = Document(caminho)
    tabelas_extraidas = []
    for tabela in doc.tables:
        data = []
        for row in tabela.rows:
            data.append([cell.text.strip() for cell in row.cells])
        df = pd.DataFrame(data)
        if not df.empty:
            df.columns = df.iloc[0]  # Usa a primeira linha como header
            df = df[1:].reset_index(drop=True)
            tabelas_extraidas.append(df)
    return tabelas_extraidas


def salvar_tabelas_em_csv(tabelas, nome_arquivo_base, metodo):
    for i, df in enumerate(tabelas):
        nome_tabela = f"{nome_arquivo_base}_tabela_{i+1}_{metodo}.csv"
        caminho_saida = os.path.join(preprocessed_folder, nome_tabela)
        df.to_csv(caminho_saida, index=False)


def processar_documento(caminho):
    arquivo = os.path.basename(caminho)
    nome_base = os.path.splitext(arquivo)[0]
    tabelas_docx = extrair_tabelas_com_python_docx(caminho)
    salvar_tabelas_em_csv(tabelas_docx, nome_base, 'python_docx')

class CSVFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.csv'):
            time.sleep(1)  # Pequeno atraso para garantir que o arquivo foi completamente escrito
            unir_arquivos_csv(preprocessed_folder)


def unir_arquivos_csv(pasta):
    arquivos_csv = [os.path.join(pasta, f) for f in os.listdir(pasta) if f.endswith('.csv')]
    dataframes = []
    
    for arquivo in arquivos_csv:
        try:
            df = pd.read_csv(arquivo)
            dataframes.append(df)
        except Exception as e:
            print(f"Erro ao ler o arquivo {arquivo}: {e}")
    
    if dataframes:
        df_unido = pd.concat(dataframes, ignore_index=True)
        caminho_saida = os.path.join(processed_folder, 'uniao_tabelas.csv')
        df_unido.to_csv(caminho_saida, index=False)
        print(f"Arquivos CSV unidos e salvos em: {caminho_saida}")
    else:
        print("Nenhum arquivo CSV encontrado para unir.")

def iniciar_monitoramento(pasta):
    observer_raw = Observer()
    observer_raw.schedule(WordFileHandler(), path=pasta, recursive=True)
    observer_raw.start()

    observer_preprocessed = Observer()
    observer_preprocessed.schedule(CSVFileHandler(), path=preprocessed_folder, recursive=True)
    observer_preprocessed.start()

    print(f"Monitorando as pastas: {pasta} e {preprocessed_folder}")
    print("Pressione Ctrl+C para parar o monitoramento.")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer_raw.stop()
        observer_preprocessed.stop()
    observer_raw.join()
    observer_preprocessed.join()


if __name__ == '__main__':
    iniciar_monitoramento(raw_folder)
