from pyspark.sql.functions import udf
from pyspark.sql.functions import isnull, isnan, col, count, when
from pyspark.sql.types import *
from pyspark.sql.types import StringType
from pyspark.sql.types import TimestampType
from babel import Locale



# 🔹 Etapa 1: Renomear colunas para formato padrão
def padronizar_colunas(df):
    colunas_limpa = [c.strip().lower().replace(" ", "_").replace("(", "").replace(")", "") for c in df.columns]
    return df.toDF(*colunas_limpa)

# Verifica nulos ou NaN (onde aplicável)
def verificar_nulos(df):
    resultado = []

    for campo in df.schema.fields:
        nome = campo.name
        tipo = campo.dataType

        if isinstance(tipo, (DoubleType, FloatType)):
            # Float ou Double → pode usar isnan
            resultado.append(count(when(isnull(col(nome)) | isnan(col(nome)), nome)).alias(nome))
        else:
            # Outros tipos → apenas isnull
            resultado.append(count(when(isnull(col(nome)), nome)).alias(nome))

    return df.select(resultado)

# 🔹 Etapa 3: Conversão de tipos (exemplo: valor_total → double, data → timestamp)
def converter_tipos(df):
    conversoes = {
"país_en":StringType(),
"código_iso":StringType(),
"moeda":StringType(),
"código_moeda":StringType(),
    }

    for coluna, tipo in conversoes.items():
        if coluna in df.columns:
            df = df.withColumn(coluna, col(coluna).cast(tipo))
    return df


# Função de tradução individual
def traduzir_nome_pais(nome_ingles):
    try:
        pais = pycountry.countries.lookup(nome_ingles)
        return locale_pt.territories.get(pais.alpha_2)
    except:
        return None

# Registrar como UDF
traduzir_nome_udf = udf(traduzir_nome_pais, StringType())

# Função para aplicar a tradução no DataFrame PySpark
def traduzir_coluna_paises(df, coluna_origem):
    return df.withColumn(traduzir_nome_udf(coluna_origem))
  

# 🔹 Etapa 4: Verificar valores inconsistentes (ex: negativos onde não deveriam)
def verificar_erros(df):
    if "valor_total" in df.columns:
        df.filter(col("valor_total") < 0).show()
    if "lucro" in df.columns:
        df.filter(col("lucro") < 0).show()

# 🔹 Etapa 5: Padronizar strings (ex: tirar espaços e colocar minúsculas)
def limpar_strings(df):
    for campo in df.schema.fields:
        if isinstance(campo.dataType, StringType):
            df = df.withColumn(campo.name, lower(trim(col(campo.name))))
    return df

# 🔹 Etapa 6: Aplicar todas as etapas
def tratar_dataframe(df):
    df = padronizar_colunas(df)
    print("🔍 Nulos por coluna:")
    display(verificar_nulos(df))
    
    df = converter_tipos(df)
    df = traduzir_coluna_paises(df, 'país')
    verificar_erros(df)
    df = limpar_strings(df)

    return df
