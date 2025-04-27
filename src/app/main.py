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
    """
    Extrai tabelas de um arquivo .docx utilizando a biblioteca python-docx.
    Args:
        caminho (str): O caminho do arquivo .docx do qual as tabelas serão extraídas.
    Returns:
        list: Uma lista de DataFrames do pandas, onde cada DataFrame representa uma tabela extraída do arquivo .docx.
    """
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
    """
    Salva uma lista de DataFrames em arquivos CSV.

    Cada DataFrame na lista `tabelas` será salvo como um arquivo CSV separado.
    O nome do arquivo será gerado com base no `nome_arquivo_base`, no índice
    do DataFrame na lista e no `metodo` fornecido.

    Args:
        tabelas (list of pandas.DataFrame): Lista de DataFrames a serem salvos.
        nome_arquivo_base (str): Nome base para os arquivos CSV.
        metodo (str): Identificador adicional para o nome do arquivo.

    Returns:
        None
    """
    for i, df in enumerate(tabelas):
        nome_tabela = f"{nome_arquivo_base}_tabela_{i+1}_{metodo}.csv"
        caminho_saida = os.path.join(processed, nome_tabela)
        df.to_csv(caminho_saida, index=False)


def processar_documento(caminho):
    """
    Processa um documento no formato DOCX, extrai tabelas e as salva como arquivos CSV.

    Args:
        caminho (str): O caminho completo para o arquivo DOCX a ser processado.
    """
    arquivo = os.path.basename(caminho)
    nome_base = os.path.splitext(arquivo)[0]
    tabelas_docx = extrair_tabelas_com_python_docx(caminho)
    salvar_tabelas_em_csv(tabelas_docx, nome_base, 'python_docx')
    print(f"\nNovo arquivo processado: {arquivo}")
    print(f"Aguardando novos arquivos na pasta: {raw}")


class WordFileHandler(FileSystemEventHandler):
    """
    Uma classe handler para monitorar e processar arquivos Word (.docx) recém-criados em um diretório.

    Métodos:
    on_created(event):
        Acionado quando um novo arquivo ou diretório é criado. Se o arquivo criado for um documento Word
        (.docx), aguarda brevemente para garantir que o arquivo foi completamente escrito e então processa o documento.

    Args: 
        event (FileSystemEvent):
        O evento de sistema de arquivos que contém informações sobre o arquivo ou diretório criado.
    """
    def on_created(self, event):
        if not event.is_directory and event.src_path.endswith('.docx'):
            time.sleep(1)  # Pequeno atraso para garantir que o arquivo foi completamente escrito
            processar_documento(event.src_path)


def iniciar_monitoramento(pasta):
    """
    Inicia o monitoramento de uma pasta específica para detectar alterações em arquivos.
    Este método utiliza a biblioteca `watchdog` para monitorar alterações em arquivos
    dentro da pasta especificada. Ele inicia um observador que permanece em execução
    até que seja interrompido manualmente (Ctrl+C).
    Args:
        pasta (str): O caminho da pasta a ser monitorada.
    Raises:
        KeyboardInterrupt: Interrompe o monitoramento quando o usuário pressiona Ctrl+C.
    """
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
    