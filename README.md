## Job Novos Endereços Cartão Devolvido

Este projeto identifica os cartões que foram devolvidos analisando os prazos de cada Courrier e envia um e-mail com a lista atualizada diariamente de cada Cliente que Alterou seu endereço, seja pelo App ou Portal, após seu cartão ser devolvido.

## CREDENCIAIS DOS BANCOS
Para rodar tem que mudar as credenciais de banco para usar o safebox
SQLServer
/src/services/db_sqlsrv

## CREDENCIAIS EMAIL

em src/settings.py tem o dicionário email, onde deve-se colocar as credenciais
para enviar o email. Neste caso usa-se o smtp

## INFORMACÕES 
para que envie para email certo em src/settings.py no dicionário COURIERS
