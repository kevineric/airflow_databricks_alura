from slack_sdk import WebClient
import pyspark.pandas as ps

slack_token = "xoxb-7624937373667-7625065611298-lbLbsXhEgzHgBQlYBatGNhsp"
client = WebClient(token=slack_token)

nome_arquivo = dbutils.fs.ls("dbfs:/databricks-results/prata/valores_reais/")[-1].name

path =  "../../../dbfs/databricks-results/prata/valores_reais/" + nome_arquivo

enviando_arquivo_csv = client.files_upload_v2(
    channel="C07K1QL3UKS",  
    title="Arquivo no formato CSV do valor do real convertido",
    file=path,
    filename="valores_reais.csv",
    initial_comment="Segue anexo o arquivo CSV:",
)

df_valores_reais = ps.read_csv("dbfs:/databricks-results/prata/valores_reais")

for moeda in df_valores_reais.columns[1:]:
    fig = df_valores_reais.plot.line(x="data", y=moeda)
    fig.write_image(f"./imagens/{moeda}.png")

def enviando_imagens(moeda_cotacao):
    enviando_imagens = client.files_upload_v2(
    channel="C07K1QL3UKS",  
    title="Enviando imagens de cotações",
    file=f"./imagens/{moeda_cotacao}.png",
)

for moeda in df_valores_reais.columns[1:]:
    enviando_imagens(moeda)
