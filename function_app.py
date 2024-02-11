import azure.functions as func
import logging
import matplotlib.pyplot as plt

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

@app.route(route="generateimage", methods=["POST"])
def generateimage(req: func.HttpRequest) -> func.HttpResponse:

    from openai import AzureOpenAI
    import json

    req_body = req.get_json()
    prompt = req_body.get('prompt')

    client = AzureOpenAI(
        api_version="2023-12-01-preview",
        azure_endpoint="https://openai4v.openai.azure.com/",
        api_key="400b33a4575348afa604e966ce0ff598",
    )

    result = client.images.generate(
        model="Dalle3",
        prompt=prompt,
        n=1
    )
    image_url = json.loads(result.model_dump_json())['data'][0]['url']



    return (image_url)

@app.route(route="file", methods=["POST"])
def file(req: func.HttpRequest) -> func.HttpResponse:

    for input_file in req.files.values():
        filename = input_file.filename
        # contents = input_file.stream.read()
        # logging.info('Filename: %s' % filename)
        # logging.info('Contents:')
        # logging.info(contents)
    
    from azure.storage.blob import BlobServiceClient
    blob_service_client = BlobServiceClient.from_connection_string("DefaultEndpointsProtocol=https;AccountName=bayergenaistorage;AccountKey=Od0wcUn4Fvk5dRMXjSZ01VkrwBPmi5s8trx2gD0zTVxA5eDT1fKT4QUBLOkZURKWHTQxOwJqzcqq+AStRQ3HVw==;EndpointSuffix=core.windows.net")
    blob_client = blob_service_client.get_blob_client(container="file-holder", blob=filename)
    blob_client.upload_blob(input_file.stream.read(), overwrite=True)
    return func.HttpResponse(f'Done\n')

    # return func.HttpResponse(input_file.stream.read(), mimetype="application/octet-stream")


@app.route(route="ask", methods=["POST"])
def ask(req: func.HttpRequest) -> func.HttpResponse:
    req_body = req.get_json()
    prompt = req_body.get('prompt')

    from langchain.chat_models import AzureChatOpenAI
    from langchain.schema import HumanMessage

    llm = AzureChatOpenAI(
        openai_api_base='https://apimbayergenai.azure-api.net/deployments/gpt4/chat/completions?api-version=2023-07-01-preview',
        openai_api_version='2023-07-01-preview',
        deployment_name='gpt4',
        openai_api_key='400b33a4575348afa604e966ce0ff598',
        openai_api_type='azure',
    )

    llm([HumanMessage(content='''You are a expert SQL data analyst. There is a table called argentina with the following schema:
    schema = StructType([
        StructField("CHANNEL", StringType(), True),
        StructField("COMMENTS", IntegerType(), True),
        StructField("COPY", StringType(), True),
        StructField("CTR", DoubleType(), True),
        StructField("Clicks", IntegerType(), True),
        StructField("DATE", TimestampType(), True),
        StructField("IMAGE", StringType(), True),
        StructField("IMPRESSIONS", IntegerType(), True),
        StructField("LIKES", IntegerType(), True),
        StructField("MONTH", StringType(), True),
        StructField("ORGANIC IMPRESSIONS", IntegerType(), True),
        StructField("PAID IMPRESSIONS", IntegerType(), True),
        StructField("REACH", IntegerType(), True),
        StructField("SAVED", IntegerType(), True),
        StructField("SHARED", IntegerType(), True),
        StructField("TOPIC", StringType(), True),
        StructField("TYPE", StringType(), True)
    ])
    ---
    Write a SQL query to get the top 10 rows by CTR.
    ''')])

    from langchain.agents.agent_types import AgentType
    from langchain_experimental.agents.agent_toolkits import create_pandas_dataframe_agent
    import pandas as pd

    file_path = 'ArgentinaBrazil_2022-2023.csv'

    # Read the CSV file using Pandas
    pandas_df = pd.read_csv(file_path)

    agent = create_pandas_dataframe_agent(
        llm,
        pandas_df,
        verbose=True,
        agent_type=AgentType.OPENAI_FUNCTIONS,
    )

    resp = agent.run(prompt)

    return (resp)


@app.route(route="db", methods=["POST"])
def db(req: func.HttpRequest) -> func.HttpResponse:
    import pymongo

    req_body = req.get_json()

    myclient = pymongo.MongoClient("mongodb://bayer-genai-server:nXbzHhyx0K1J340qgUNUGQaoj4oZpQnCDIdkjmfYffWzpRnaGzMJhFzFDi9oKf7lqYPO7EgTiY2dACDbANIGag==@bayer-genai-server.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@bayer-genai-server@")
    mydb = myclient["bayer-genai-database"]
    mycol = mydb["Thumbs"]

    book = {
        "DOWN": "1",
        "UP": "1"
    }

    resp = mycol.insert_one(req_body)

    # for b in mycol.find():
    #     print(b)

    return("OK")