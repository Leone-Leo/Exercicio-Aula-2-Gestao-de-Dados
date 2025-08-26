import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import re
from datetime import datetime
import os

def extrair_nome_tabela(sql_content):
    match = re.search(r'CREATE TABLE\s+(\w+)', sql_content, re.IGNORECASE)
    return match.group(1) if match else None

def gerar_sql_insert(df, nome_tabela):
    output_sql_path = os.path.join('saida', 'dados_insercao.sql')
    inserts = []
    for _, row in df.iterrows():
        colunas = ', '.join(row.index)
        valores = ', '.join([f"'{str(v).replace('\'', '\'\'')}'" if pd.notna(v) else 'NULL' for v in row.values])
        inserts.append(f"INSERT INTO {nome_tabela} ({colunas}) VALUES ({valores});")
    
    with open(output_sql_path, 'w', encoding='utf-8') as f:
        f.write('\n'.join(inserts))
    print(f"SQL de inserção gerado em: {output_sql_path}")

def analisar_completude(df):
    return (df.isnull().sum() / len(df)) * 100

def analisar_unicidade(df):
    erros_unicidade = {}
    if 'ID_Pedido' in df.columns:
        duplicados = df.duplicated(subset=['ID_Pedido']).sum()
        erros_unicidade['ID_Pedido_duplicado'] = (duplicados / len(df)) * 100
    return pd.Series(erros_unicidade)

def analisar_validade(df):
    erros_validade = {
        'preco_nao_numerico': 0,
        'data_formato_invalido': 0,
        'cep_formato_invalido': 0
    }
    precos_invalidos = pd.to_numeric(df['Preco_Unitario'], errors='coerce').isnull() & df['Preco_Unitario'].notnull()
    erros_validade['preco_nao_numerico'] = (precos_invalidos.sum() / len(df)) * 100

    datas_invalidas = pd.to_datetime(df['Data_Compra'], format='%Y-%m-%d', errors='coerce').isnull() & df['Data_Compra'].notnull()
    erros_validade['data_formato_invalido'] = (datas_invalidas.sum() / len(df)) * 100

    cep_regex = re.compile(r'^\d{8}$')
    ceps_invalidos = df['CEP_Entrega'].notnull() & ~df['CEP_Entrega'].astype(str).str.match(cep_regex)
    erros_validade['cep_formato_invalido'] = (ceps_invalidos.sum() / len(df)) * 100
    
    return pd.Series(erros_validade)

def analisar_consistencia(df):
    erros_consistencia = {}
    sku_nome_map = df.dropna(subset=['SKU_Produto', 'Nome_Produto']).groupby('SKU_Produto')['Nome_Produto'].nunique()
    skus_inconsistentes = sku_nome_map[sku_nome_map > 1].count()
    erros_consistencia['sku_com_nomes_diferentes'] = (skus_inconsistentes / df['SKU_Produto'].nunique()) * 100
    return pd.Series(erros_consistencia)

def analisar_acuracia(df):
    erros_acuracia = {}
    precos = pd.to_numeric(df['Preco_Unitario'], errors='coerce')
    quantidades = pd.to_numeric(df['Quantidade'], errors='coerce')
    fretes = pd.to_numeric(df['Custo_Frete'], errors='coerce')
    datas = pd.to_datetime(df['Data_Compra'], errors='coerce')

    erros_acuracia['preco_zerado_ou_nulo'] = ((precos == 0) | precos.isnull()).sum() / len(df) * 100
    erros_acuracia['quantidade_negativa'] = (quantidades < 0).sum() / len(df) * 100
    erros_acuracia['frete_negativo'] = (fretes < 0).sum() / len(df) * 100
    erros_acuracia['data_compra_futura'] = (datas > datetime.now()).sum() / len(df) * 100
    
    return pd.Series(erros_acuracia)

def plotar_qualidade_dados(metricas, titulo, output_path):
    plt.figure(figsize=(12, 8))
    plt.title(titulo, fontsize=16)

    df_metricas = pd.DataFrame(metricas).reset_index()
    df_metricas.columns = ['Métrica', 'Percentual de Problemas']
    
    sns.barplot(x='Percentual de Problemas', y='Métrica', data=df_metricas, palette='viridis', hue='Métrica', legend=False)
    plt.xlabel('Percentual de Problemas (%)', fontsize=12)
    plt.ylabel('Métrica/Coluna', fontsize=12)
    
    max_val = df_metricas['Percentual de Problemas'].max()
    plt.xlim(0, max(10, max_val * 1.2))
    
    for index, value in enumerate(df_metricas['Percentual de Problemas']):
        plt.text(value + (max(1, max_val) * 0.02), index, f'{value:.2f}%', va='center')
            
    plt.tight_layout()
    plt.savefig(os.path.join(output_path, f"{titulo.lower().replace(' ', '_')}.png"))
    plt.close()

def plotar_unicidade(percentual_duplicatas, output_path):
    labels = ['Dados Únicos', 'Dados Duplicados']
    sizes = [100 - percentual_duplicatas, percentual_duplicatas]
    colors = ['#4CAF50', '#FFC107']
    explode = (0, 0.1) if percentual_duplicatas > 0 else (0, 0)

    plt.figure(figsize=(8, 8))
    plt.pie(sizes, explode=explode, labels=labels, colors=colors, autopct='%1.1f%%',
            shadow=True, startangle=140)
    plt.title('Análise de Unicidade', fontsize=16)
    plt.axis('equal')
    plt.savefig(os.path.join(output_path, 'analise_unicidade.png'))
    plt.close()

def main(csv_path, sql_path, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    try:
        df = pd.read_csv(csv_path)
        with open(sql_path, 'r') as f:
            sql_content = f.read()
    except FileNotFoundError as e:
        print(f"Erro: Arquivo não encontrado - {e.filename}")
        return
    except Exception as e:
        print(f"Ocorreu um erro ao ler os arquivos: {e}")
        return

    nome_tabela = extrair_nome_tabela(sql_content)
    if not nome_tabela:
        print("Erro: Não foi possível extrair o nome da tabela do arquivo SQL.")
        return
    
    gerar_sql_insert(df, nome_tabela)
    
    print("Iniciando análises de qualidade de dados...")
    completude = analisar_completude(df)
    unicidade_series = analisar_unicidade(df)
    validade = analisar_validade(df)
    consistencia = analisar_consistencia(df)
    acuracia = analisar_acuracia(df)
    
    print("Gerando gráficos...")
    plotar_qualidade_dados(completude, 'Análise de Completude', output_dir)
    plotar_unicidade(unicidade_series.iloc[0] if not unicidade_series.empty else 0, output_dir)
    plotar_qualidade_dados(validade, 'Análise de Validade', output_dir)
    plotar_qualidade_dados(consistencia, 'Análise de Consistência', output_dir)
    plotar_qualidade_dados(acuracia, 'Análise de Acurácia', output_dir)

    print(f"Processo concluído. Verifique os arquivos na pasta '{output_dir}'.")

if __name__ == '__main__':
    INPUT_DIR = 'entrada'
    OUTPUT_DIR = 'saida'
    if not os.path.exists(INPUT_DIR):
        os.makedirs(INPUT_DIR)

    csv_file_path = os.path.join(INPUT_DIR, 'vendas_eletronicos.csv')
    sql_file_path = os.path.join(INPUT_DIR, 'vendas_schema.sql')

    sql_schema_data = """
CREATE TABLE Vendas (
    ID_Pedido INT,
    Data_Compra DATE,
    SKU_Produto VARCHAR(50),
    Nome_Produto VARCHAR(100),
    Categoria VARCHAR(50),
    Preco_Unitario DECIMAL(10, 2),
    Quantidade INT,
    Custo_Frete DECIMAL(10, 2),
    CEP_Entrega VARCHAR(10),
    Status_Entrega VARCHAR(20)
);
"""
    with open(sql_file_path, 'w', encoding='utf-8') as f:
        f.write(sql_schema_data)

    if not os.path.exists(csv_file_path):
        print(f"ERRO: Arquivo 'vendas_eletronicos.csv' não encontrado na pasta '{INPUT_DIR}'.")
        print("Por favor, crie o arquivo a partir do artefato fornecido e execute novamente.")
    else:
        main(csv_path=csv_file_path, sql_path=sql_file_path, output_dir=OUTPUT_DIR)

