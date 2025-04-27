import os
import glob
import time
import pandas as pd
from docx import Document
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

# Caminhos
raw = './data/raw'
processed = './data/processed'
os.makedirs(processed, exist_ok=True)


def extrair_tabelas_com_python_docx(caminho):
    """Extrai todas as tabelas de um .docx usando python-docx."""
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
    """Salva as tabelas extraídas em arquivos CSV."""
    for i, df in enumerate(tabelas):
        nome_tabela = f"{nome_arquivo_base}_tabela_{i+1}_{metodo}.csv"
        caminho_saida = os.path.join(processed, nome_tabela)
        df.to_csv(caminho_saida, index=False)


def processar_documento(caminho):
    """Processa o documento Word e salva as tabelas extraídas."""
    arquivo = os.path.basename(caminho)
    nome_base = os.path.splitext(arquivo)[0]
    tabelas_docx = extrair_tabelas_com_python_docx(caminho)
    salvar_tabelas_em_csv(tabelas_docx, nome_base, 'python_docx')
    print(f"\nNovo arquivo processado: {arquivo}")
    print(f"Aguardando novos arquivos na pasta: {raw}")


class WordFileHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.docx'):
            time.sleep(1)  # Pequeno atraso para garantir que o arquivo foi completamente escrito
            processar_documento(event.src_path)


def iniciar_monitoramento(pasta):
    observer = Observer()
    observer.schedule(WordFileHandler(), path=pasta, recursive=True)
    observer.start()
    print("Monitoramento iniciado. Pressione Ctrl+C para parar.")
    print(f"Monitorando a pasta: {pasta}")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()
    observer.join()


if __name__ == '__main__':
    os.makedirs(raw, exist_ok=True)
    iniciar_monitoramento(raw)
    