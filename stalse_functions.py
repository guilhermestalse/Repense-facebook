import pandas_gbq
import pandas as pd
import datetime as dt
from google.cloud import storage, secretmanager


def keygcp(secret_id, project_id="dev-stalse"):
    """Função para consumir as chaves secretas que estão cadastradas no GCP

    Args:
        secret_id (str): é o nome da chave dentro do  KMS
        project_id (str, opcional): está por default o projeto "dev-stalse", mas se for necessário poderá ser alterado na chamada da função

    Returns:
        str: Valor da key solicitada
    """
    # declarando variaveis para uso na API KMS
    _secret_id = secret_id
    _project_id = project_id
    # Incializando client KMS
    secretmanager_client = secretmanager.SecretManagerServiceClient()
    # requisitando KMS
    response=secretmanager_client.access_secret_version(
        name=f'projects/{_project_id}/secrets/{_secret_id}/versions/latest' # ultima versao da key
    )
    # retornando valor da secret
    key = response.payload.data.decode("UTF-8")
    return key


def cria_bq(df, id_tabela, if_exists, project, location='us-east4', table_schema=0):
    """Função que cria uma tabela no BigQuery

    Args:
        df (DataFrame): Dataframe que possui os dados para importar no BigQuery
        id_tabela (str): o Id ta tabela é composto por (`projeto_id.conjunto_de_dados.tabela`)
        if_exists (str): Escolher se a tabela vai fazer "replace", ou "append"
        project_id (str): ID do projeto em questão
        location (str, optional): Localização para criar a tablela no BigQuery. Defaults to 'us-east4'.
        table_schema (list of dict, optional): Quando a tabela no BigQuery precisa de um schema expecifico. Defaults to 0.
        
    Returns:
        str: retorna a quantidade de linhas importadas no BigQuery
    """
    
    # acrescentando apenas id's que a API retornar na requisição
    print('Pronto para inserir os dados no BigQuery.')
    if len(df)> 0:
        
        # tratando a string id_tabela e project
        id_tabela = id_tabela.replace("`", "")
        project = project.replace('`', '')
        
        # importando dados no BQ
        print('Inserindo {} linhas a tabela de dados agregados.'.format(len(df)))
        
        # validando se existe table_schema
        if table_schema == 0:
            pandas_gbq.to_gbq(df,
                            id_tabela,  
                            if_exists=if_exists,
                            location=location,
                            project_id=project,
                            progress_bar=True)
            
        else:
            pandas_gbq.to_gbq(df,
                id_tabela,  
                if_exists=if_exists,
                table_schema=table_schema,
                location=location,
                project_id=project,
                progress_bar=True)
            
        info_ok = 'Todas as {} linhas foram inseridas na tabela.'.format(len(df))
        return info_ok
    else:
        info_nok = 'Não há novas linhas a serem inseridas.'
        return info_nok


def deleta_ids(df, coluna, id_tabela, project):
    """Função que deleta somente os ID's que a API retornou

    Args:
        df (DataFrame): Dataframe que possui os ID's
        coluna (str): Coluna referente ao ID
        project (str): ID do projeto em questão
        id_tabela (str): o Id ta tabela é composto por (`projeto_id.conjunto_de_dados.tabela`)
        location (str, optional): Localização para deletar a tablela no BigQuery. Defaults to 'us-east4'.

    Returns:
        str: Mensagem de exclusão
    """

    project = project.replace('`', '')
    
    # selecionado somente os ID's para para deleta-los futuramente
    del_ids = list(df[coluna])
    
    # tratando ids para inserir no comando SQL
    del_ids = str(del_ids).replace('[', '(')
    del_ids = str(del_ids).replace(']', ')')
    del_ids = str(del_ids).replace("'", "")
    
    print('Deletando {} ids do BQ'.format(len(df)))

    sql_del_ids = "delete from "+id_tabela+" where "+coluna+" in {}".format(del_ids)

    # executando a deleção
    sql_del_ids = pandas_gbq.read_gbq(sql_del_ids, project_id=project)
    
    return "ID's excluidos no banco"


def deleta_datas(df, coluna_data, id_tabela, project, format_date='%Y-%m-%d'):
    """Função que deleta a quantidade de datas que contém na variável 'del_datas'

    Args:
        df (DataFrame): Dataframe que possui as datas
        coluna_data (str): Coluna referente a data
        id_tabela (str): o Id ta tabela é composto por (`projeto_id.conjunto_de_dados.tabela`)
        project (str): ID do projeto em questão
        format_date (str, optional): Formato de data que o DataFrame possui. Defaults to '%Y-%m-%d'.

    Returns:
        str: Mensagem de exclusão
    """
    
    project = project.replace('`', '')
    
    # realizando e printando uma lista das datas únicas
    df[coluna_data] = pd.to_datetime(df[coluna_data], infer_datetime_format=True).dt.date
    del_datas = [d.strftime(format_date) for d in list(df[coluna_data].unique())]
    
    # formatando a lista de dias para deletar do BQ
    days_query = "', '".join(del_datas)
    days_query = "('"+days_query+"')"
    
    # deletando as colunas do BQ de acordo com as datas que está na variável "days_query"
    sql_del_days = "delete from "+id_tabela+" where "+coluna_data+" in {}".format(days_query)

    pandas_gbq.read_gbq(sql_del_days, project_id=project)
    
    return 'Excluindo os ultimos {} dias do BIGQUERY: {}'.format(len(del_datas), del_datas)
