from services.db_sqlsrv import SQLServer
from services.pier import Pier
from jobs.getAddressPerson import get_address
from datetime import datetime, timedelta

from settings import ARQUIVOS_TEMPORARIOS, COURIERS
from services.email_sender import send_email

import csv
import os

def atualizar_enderecos(db_rastreamento_cartoes, courier_id, days_before_now, receiver_email, filename, subject, body):
    
    db_cursor = db_rastreamento_cartoes.connect()
    pier = Pier()
    days_before_now = -(days_before_now)    
    cmd = """
      SELECT objetos.id
        ,objetos.data_postagem
        ,objetos.data_baixa
        ,objetos.id_externo
        ,objetos.id_conta
        ,objetos.flg_devolvido
        ,objetos.data_atualizacao
      ,enderecos.id
      ,enderecos.rua
      ,enderecos.bairro
      ,enderecos.cidade
      ,enderecos.uf
      ,enderecos.cep
      ,his.hawb_id
    FROM objetos
    INNER JOIN (select * from historico_objetos where id in (select max(id) from historico_objetos group by objeto_id)) his on objetos.id = his.objeto_id
    JOIN enderecos on enderecos.id = objetos.endereco_id

    WHERE 
    flg_entregue != 1 
    and flg_devolvido = 1
    and data_baixa > DATEADD(day, ?, DATEDIFF(day, 0, GETDATE()))
    and courier_id = ?
    and enderecos.id = objetos.endereco_id
    order by data_baixa DESC;
  """

    db_cursor.execute(cmd, days_before_now, courier_id)
    registros_total_express = db_cursor.fetchall()
    filepath_remessa = os.path.join(ARQUIVOS_TEMPORARIOS, filename)
    with open(filepath_remessa, 'w', encoding='UTF8', newline="\n") as file:
        writer = csv.writer(file, delimiter=";")
        HEADER = ["ID_CONTA", "TIPO_ENDERECO", "NOME", "ENDERECO",
                  "BAIRRO", "CIDADE", "ESTADO", "CEP", "COD_RASTREIO", "AWB"]
        writer.writerow(HEADER)

        for registro in registros_total_express:
            id_conta = registro.id_conta

            print(id_conta)
            status_request, address_request, conta_request = get_address(
                id_conta, pier)

            if status_request != 200:
                continue

            address = address_request["content"][0]
            dataUltimaAtualizacao = address["dataUltimaAtualizacao"]

            if dataUltimaAtualizacao != None:
                address_ultima_atualizacao = dataUltimaAtualizacao.replace(
                    "Z", "")
                ultima_atualizacao = datetime.fromisoformat(
                    address_ultima_atualizacao)


                hoje = datetime.today().replace(hour=0, minute=0, second=0, microsecond=0)
                ontem = hoje - timedelta(days=1)

                if(registro.data_postagem < ultima_atualizacao and ontem <= ultima_atualizacao <= hoje):

                    antigo_endereco = [registro.id_conta, "ANTIGO", conta_request["nome"], registro.rua,
                                       registro.bairro, registro.cidade, registro.uf, registro.cep, registro.id_externo, registro.hawb_id]

                    endereco_completo_novo = f"{address['logradouro']} {address['numero']} {address['complemento']}"

                    novo_endereco = [registro.id_conta, "NOVO", conta_request["nome"], endereco_completo_novo, address["bairro"],
                                     address["cidade"], address["uf"], address["cep"], registro.id_externo,  registro.hawb_id]
                    writer.writerow(antigo_endereco)
                    writer.writerow(novo_endereco)

    send_email(receiver=receiver_email, filename=filename, subject=subject, body=body)

def main():      
    db_rastreamento_cartoes = SQLServer()
    for courier, item in COURIERS.items():
        print(f"Enviando para {courier}")

        courier_id = item["id"]
        dias = item["dias_anterior"]
        email= item["enviar_para"]
        subject = item["subject"]
        body = item["body"]
       
        atualizar_enderecos(db_rastreamento_cartoes=db_rastreamento_cartoes, courier_id=courier_id, days_before_now=dias, receiver_email=email, filename="remessa.csv", subject=subject, body=body)

    db_rastreamento_cartoes.close()

if __name__=='__main__':
    main()