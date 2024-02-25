
from pandas import DataFrame 
def normalize_md_file(dataframe):
    # itens da proposta estão a partir da linha 23
    proposta = dataframe[23:]
    dict_resultado = {}
    proposta_descricao_do_item = []
    proposta_marca = []
    proposta_preco = []


    for i in range(len(proposta)):
        indice = proposta.iloc[i].values[[5, 15, 21]]

        if '< Registro ANVISA : ' in str(indice[0]):
            proposta_descricao_do_item.append(indice[0])
            proposta_marca.append(indice[1])
            proposta_preco.append(indice[2])


    dict_resultado['descricao'] = proposta_descricao_do_item
    dict_resultado['marca'] = proposta_marca
    dict_resultado['preco_unitario'] = proposta_preco
    proposta = DataFrame(dict_resultado)

    # Extrai só o registro da planilha de descrição do item
    temp = proposta['descricao'].apply(
    lambda x: str(x).split('Registro ANVISA :')[1])
    temp2 = list(temp.apply(lambda x: str(x).split('>')[0]))
    proposta['registro'] = list([int(x.strip()) for x in temp2])

    # faz split em '< Registro ANVISA : ' e pega o primeiro item
    temp_desc = proposta['descricao'].apply(
    lambda x: str(x).split('< Registro ANVISA :')[0])
    proposta['descricao'] = temp_desc
  
    return proposta



