from pyspark.sql.functions import col, trim, lower, when, count, isnan
from pyspark.sql.types import DoubleType, FloatType, IntegerType, TimestampType, StringType



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
  "tipos_de_clientes":string(),
  "país":string(),
  "produto":string(),
  "valor_total":double(),
  "desconto":double(),
  "valor_total_c/_desconto":double(),
  "custo_total":double(),
  "lucro":double(),
  "data":datetime(),
  "mês":string(),
  "ano":int(),
    }

    for coluna, tipo in conversoes.items():
        if coluna in df.columns:
            df = df.withColumn(coluna, col(coluna).cast(tipo))
    return df

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
    verificar_erros(df)
    df = limpar_strings(df)
    
    return df
