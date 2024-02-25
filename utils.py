
from app import app
from unidecode import unidecode
from difflib import SequenceMatcher
from re import sub
import pandas as pd
import os

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
    proposta = pd.DataFrame(dict_resultado)

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


def confirmar_marca_do_registro(marca_proposta: str, marca_tabela: str) -> bool:
    """
    Compara a marca da proposta com a marca da tabela CMED

    Parâmetros: 

        marca_proposta (str): A marca do item que está na proposta.
        marca_tabela (str): A marca do item que está na tabela.

    Retorno:

    (bool): True se a marca é a mesma False se não.

    """
    texto1 = unidecode(marca_proposta.upper())
    texto2 = unidecode(marca_tabela.upper())

    if texto1 in texto2:
        return True

    return False

def comparar_apresentacao_do_registro(apresentacao_proposta: str, apresentacao_tabela: str) -> bool:

    apresentacao_proposta_tratada = unidecode(_remover_caracteres(apresentacao_proposta).lower())
    apresentacao_tabela_cmed_tratada = unidecode(_remover_caracteres(apresentacao_tabela).lower())
    # Calcula a similaridade entre as strings de descrição
    similarity_ratio = SequenceMatcher(None, apresentacao_proposta_tratada, apresentacao_tabela_cmed_tratada).ratio()
    # print(f'COMPARAÇÃO:\n1: {apresentacao_proposta_tratada} \n2: {apresentacao_tabela_cmed_tratada}')
    # print(f'Similaridade: {round(similarity_ratio, 2)}')
    return round(similarity_ratio, 2) > 0.5

def comparar_preco_unitario_do_registro(preco_proposta: float, preco_tabela: float) -> bool:
    preco_tabela = float(str(preco_tabela).replace(',', '.'))
    # print(f'COMPARAÇÃO:\nPreço proposta: {preco_proposta} \nPreço tabela: {preco_tabela}')

    return preco_tabela >= preco_tabela

def tratar_tabela_cmed(tabela_cmed):

    # Cria uma coluna com a substância
    tabela_cmed['DESCRIÇÃO'] = tabela_cmed['SUBSTÂNCIA'] + ' ' + tabela_cmed['APRESENTAÇÃO']
    # Cria uma columa com a quantidade 
    tabela_cmed['quantidade'] = _calcular_quantidade(tabela_cmed)
    # Cria uma coluna com o preço unitário
    tabela_cmed['PRECO_UNITARIO_BA'] = _calcular_preco_unitario(tabela_cmed)

    tabela_cmed.to_csv(os.path.join("tabelas", "tabela_cmed_tratada.csv"), index=False, sep=';')

    return tabela_cmed

def _remover_caracteres(string):
    # Expressão regular para pegar apenas letras, números ou o caractere "/"
    padrao = r'[^A-Za-z0-9/]'
    # Substitui os caracteres indesejados por uma string vazia
    string_limpa = sub(padrao, '', string)
    return string_limpa

def _calcular_preco_unitario(tabela_cmed):
    preco_unit = []
    for i, preco in enumerate(tabela_cmed['PF 18%']):

        quantidade = tabela_cmed['quantidade'].iloc[i]

        if quantidade != -1:

            try: 
                aux = str(float(str(preco).replace(',', '.')) / int(quantidade)).replace('.', ',') #format(float(str(preco).replace(',', '.')) / int(quantidade), '.2f')
                preco_unit.append(aux)

            except:
                preco_unit.append('indisponivel')
        else:

            preco_unit.append('indisponivel')

    return preco_unit

def _calcular_quantidade(tabela_cmed):
    # coletando as quantidades do item de cada apresentacao
    apresentacoes = tabela_cmed['APRESENTAÇÃO'].values
    quant = []
    for i in apresentacoes:
        quant.append(_pegar_quantidade_da_apresentacao(i))
    return quant

def _pegar_quantidade_da_apresentacao(apresentacao: str) -> int:

    """
    Extrai a quantidade do item com base na apresentação.

    Parâmetros: 

        apresentacao (str): A apresentacao do item da tabela CMED.

    Retorno:

        (int): Quantidade extraída da apresentação. Se não achar retorna -1.

    """

    import re
    texto = apresentacao.strip()

    # Padrões da quantidade na apresentação
    padrao_quantidade_no_meio_com_cx = re.compile(r"CX \d{1,3}")
    padrao_quantidade_no_final = re.compile(r" X \d{1,3}$")
    padrao_quantidade_no_meio_com_ct = re.compile(r"CT \d{1,3}")

    # Matches (lista com o retorno do padrão encontrado)
    matches_quantidade_no_meio_com_cx = padrao_quantidade_no_meio_com_cx.findall(texto)
    matches_quantidade_no_final = padrao_quantidade_no_final.findall(texto)
    matches_quantidade_no_meio_com_ct = padrao_quantidade_no_meio_com_ct.findall(texto)


    # Quantidade é o valor do final multiplicado pelo do meio CT: Quantidade = "ct 10 x 2 " =  20
    if len(matches_quantidade_no_final) and len(matches_quantidade_no_meio_com_ct):

        #print(' # Quantidade é o valor do final multiplicado pelo do meio CT: Quantidade = "ct 10 x 2 " =  20')
        quantidade_final = re.sub('[^0-9]', '', matches_quantidade_no_final[0])
        quantidade_meio_ct =  re.sub('[^0-9]', '', matches_quantidade_no_meio_com_ct[0])

        try: 
            return int(quantidade_final) * int(quantidade_meio_ct)
        except:
            return -1
    
    # Quantidade é o valor do final multiplicado pelo do meio CX: Quantidade = "cx 10 x 2 " =  20
    elif len(matches_quantidade_no_final) and len(matches_quantidade_no_meio_com_cx):
        #print('  # Quantidade é o valor do final multiplicado pelo do meio CX: Quantidade = "cx 10 x 2 " =  20')
        quantidade_final = re.sub('[^0-9]', '', matches_quantidade_no_final[0])
        quantidade_meio_cx =  re.sub('[^0-9]', '', matches_quantidade_no_meio_com_cx[0])

        try: 
            return int(quantidade_final) * int(quantidade_meio_cx)
        except:
            return -1
    
    # Quantidade só no final
    elif len(matches_quantidade_no_final) and not (len(matches_quantidade_no_meio_com_ct) or len(matches_quantidade_no_meio_com_cx)):

        #print('# Quantidade só no final')
        quantidade_final = re.sub('[^0-9]', '', matches_quantidade_no_final[0])

        try: 
            return int(quantidade_final)
        except:
            return -1
    
    # Quantidade só no meio com ct
    elif not len(matches_quantidade_no_final) and (len(matches_quantidade_no_meio_com_ct)):
        #print('# Quantidade só no meio com ct')
        quantidade_meio_ct = re.sub('[^0-9]', '', matches_quantidade_no_meio_com_ct[0])

        try: 
            return int(quantidade_meio_ct)
        except:
            return -1
    
    # quantidade só no meio com cx
    elif not len(matches_quantidade_no_final) and (len(matches_quantidade_no_meio_com_cx)):
        
        #print('# quantidade só no meio com cx')
        #print(f' len qtd final: {len(matches_quantidade_no_final)}')
        quantidade_meio_cx = re.sub('[^0-9]', '', matches_quantidade_no_meio_com_cx[0])

        try: 
            return int(quantidade_meio_cx)
        except:
            return -1
    
    # Quantidade não encontrada
    else:
        """
        A quantidade não foi encontrada e por isso pode ser 1 ou não deu match na regex
        Podemos considerar 1 porque se o preço tabelado for menor que o preço da proposta
        o item será aprovado independente da quantidade, se o preço tabelado for maior que
        o preço da proposta o item precisa de confirmação manual.
        """
        return 1 
