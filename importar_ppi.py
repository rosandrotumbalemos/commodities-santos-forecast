import pandas as pd
import mysql.connector
from datetime import datetime
import requests
from io import BytesIO

# --- CONFIGURAÇÕES DO BANCO ---
DB_CONFIG = {
    'host': 'banco-de-dados-analise-commodities.cals6oigkokx.us-east-1.rds.amazonaws.com',
    'user': 'rosandro',
    'password': '****',
    'database': '****'
}

# Link oficial da ANP
URL_ANP = "https://www.gov.br/anp/pt-br/assuntos/precos-e-defesa-da-concorrencia/precos/arq-ppi/ppi.xlsx"

print("⏳ Iniciando extração retificada: Aba 'Gasolina R$ semanal' (Porto de Santos)...")

try:
    # 1. Download do arquivo
    response = requests.get(URL_ANP)
    
    # 2. Leitura da aba específica (skiprows=2 para alinhar com o cabeçalho real)
    df = pd.read_excel(BytesIO(response.content), sheet_name='Gasolina R$ semanal', skiprows=2)
    
    # 3. Seleção por Posição (Coluna B é índice 1, Coluna G de Santos é índice 6)
    # Isso garante que pegamos a 'Data' e o preço de 'Santos' independente do nome
    df_focado = df.iloc[:, [1, 6]].copy()
    df_focado.columns = ['Data_Texto', 'Valor_Santos']
    
    # 4. Limpeza de nulos
    df_focado = df_focado.dropna()
    
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Opcional: Limpar a tabela antes para evitar lixo de 2018/2019
    cursor.execute("TRUNCATE TABLE mercado_etanol")
    
    count = 0
    data_inicio_projeto = datetime(2020, 1, 1).date()

    # 5. Loop de Processamento
    for _, linha in df_focado.iterrows():
        try:
            # Tratamento da String de Data (ex: "05/01/2020 a 11/01/2020")
            data_raw = str(linha['Data_Texto']).strip()
            data_limpa = data_raw.split(' ')[0].strip() # Pega só a primeira data
            data_obj = datetime.strptime(data_limpa, '%d/%m/%Y').date()
            
            # Filtro: 2020 até 2026
            if data_obj >= data_inicio_projeto:
                preco = float(linha['Valor_Santos'])
                
                # Inserção no Banco
                sql = "INSERT INTO mercado_etanol (data_referencia, preco_gasolina_refinaria) VALUES (%s, %s)"
                cursor.execute(sql, (data_obj, preco))
                count += 1
        except Exception:
            continue

    conn.commit()
    print(f"✅ SUCESSO! {count} semanas carregadas (Período: 2020-2026).")
    print(f"📍 Dados extraídos do Porto de SANTOS.")

except Exception as e:
    print(f"❌ Erro na retificação: {e}")
finally:
    if 'conn' in locals() and conn.is_connected():
        cursor.close()
        conn.close()
