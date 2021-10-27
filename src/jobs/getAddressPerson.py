def get_address(idConta, pier):
    url_conta = f"/contas/{idConta}"
    status_conta, conta = pier.get(url=url_conta)
    if status_conta == 200:
        idPessoa = conta["idPessoa"]
        url_address = f"/enderecos?idPessoa={idPessoa}"
        status_address, address = pier.get(url=url_address)
        return status_address, address, conta
    return status_conta, None, None

    


