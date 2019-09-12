import json
from multiprocessing import freeze_support
from multiprocessing.pool import Pool
from time import sleep

import requests
from bs4 import BeautifulSoup
from pymongo import MongoClient
from requests import Request

#MONTA REQUEST PARA AUTENTICACAO
def make_auth_request():
    headers = {
        "Accept": "application/json, text/javascript, */*; q=0.01",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Connection": "keep-alive",
        "Content-Length": "350",
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Cookie": "PHPSESSID=3fmdk8iqtbpkn9krc3htf8uf62; __ib2pgvar_368=a; ac_enable_tracking=1; _ga=GA1.2.962157374.1560095186; _gid=GA1.2.23224614.1560095186; _fbp=fb.1.1560095186614.1439555223; __ib2pgvar_814=a; __ib2pgses_814_a=1560095207; __ib2vid=KRmfioAM; fbm_1034655236569177=base_domain=.eduardocavalcanti.com; __ib2pgvar_360=a; wordpress_test_cookie=WP+Cookie+check; __ib2pgvar_464=a; __ib2pgses_464_a=1560095232; _gat_gtag_UA_55637572_3=1; arm_cookie_83530=3fmdk8iqtbpkn9krc3htf8uf62%7C%7C355473; arm_autolock_cookie_83530=3fmdk8iqtbpkn9krc3htf8uf62%7C%7C355473; hotid=eyJjaWQiOiIxNTYwMDk1MTg4MDgyNzkxOTU3NTU0NTY5NjU1NTAiLCJiaWQiOiIxOTk3ZmM0MDk5MDZkZTExODkxYjQ2YmE0ZWNiMjM5NSIsInNpZCI6ImUzYjgxODM0ZWQ2YTQ5MWJhMGUxM2M4ZGE2MDQwZTg1In0=",
        "Host": "eduardocavalcanti.com",
        "Origin": "https://eduardocavalcanti.com",
        "Referer": "https://eduardocavalcanti.com/login/",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36",
        "X-Requested-With": "XMLHttpRequest"
    }


    data = {
        "action": "arm_shortcode_form_ajax_action",
        "form_random_key": "102_wzwgoI08CB",
        "user_login": "wilquintanilha@gmail.com",
        "user_pass": "41545448",
        "rememberme": "",
        "arm_action": "please-login",
        "redirect_to": "https://eduardocavalcanti.com",
        "isAdmin": "0",
        "referral_url": "https://eduardocavalcanti.com/login/",
        "form_filter_kp": "11",
        "form_filter_st": "1560110831",
        "arm_nonce_check": "6b39c55dcc"
    }

    url = "https://eduardocavalcanti.com/wp-admin/admin-ajax.php"
    return Request('POST', url, data=data, headers=headers).prepare()



#PROCESSA PAGINA COM AS INFORMACOES DA EMPRESA
def process_dashboard(soup):
    # DADOS DA EMPRESA
    dados_da_empresa = soup.find_all(class_="infoDados")[0].find_all("tr")
    dict_dados_da_empresa ={}
    for tr in dados_da_empresa:
        dict_dados_da_empresa[tr.find(class_="th_left").b.text.replace(":","")] = tr.find(class_="text-center").b.text

    # GOVERNANCA
    governanca = soup.find_all(class_="infoDados")[1].find_all("tr")
    dict_governaca = {}
    for tr in governanca:
        dict_governaca[tr.find(class_="th_left").b.text.replace(":","")] = tr.find(class_="text-center").b.text

    #ANALISE
    analise_th = soup.find_all("th")
    colunas_tabela_analise = []
    for th in analise_th:
        colunas_tabela_analise.append(th.span.text.replace(".",""))

    linhas_tabela_analise = []
    analise_tr = soup.find(class_="analise").find("tbody").find_all("tr")
    analise_tr.pop()
    for tr in analise_tr:
        linha = []
        colunas = tr.find_all("td")
        for item in colunas:
            if item.b is not None:
                linha.append(item.b.text)
            else:
                linha.append(item.text)

        linhas_tabela_analise.append(linha)

    list_analise =[]
    for index_linha in range(len(linhas_tabela_analise)):
        dict_row = {}
        for index_coluna in range(len(colunas_tabela_analise)):
            linha = linhas_tabela_analise[index_linha]
            dict_row[colunas_tabela_analise[index_coluna]] = linha[index_coluna]
        list_analise.append(dict_row)


    return {"dados_da_empresa":dict_dados_da_empresa,
             "governanca":dict_governaca,
             "analise_fundamentalista": list_analise
            }


def processing(href):
    try:
        text = session.get(href).text
        resultado = process_dashboard(BeautifulSoup(text, features="html.parser"))
        collection.insert_one(resultado)
        print("INSERIDO")
        return True
    except:
        return False

def process_and_insert(href):
    while not processing(href):
        print("retry: " + href)







#INICIA SESSAO CLIENT MONGO
cliente = MongoClient("mongodb+srv://admin:admin@cluster0-zfnw0.mongodb.net/test?retryWrites=true&w=majority")
database = cliente.test_crawler
collection = database.test_crawler_collection

#INICIA SESSAO REQUESTS
session = requests.session()

#REALIZA AUTENTICACAO
response_login = session.send(make_auth_request())

#ACESSA DASHBOARD
response_dashboard = session.get("https://eduardocavalcanti.com/dashboard/")
soup = BeautifulSoup(response_dashboard.text,features="html.parser")

#CAPTURA TODAS AS ACOES A SEREM PROCESSADAS
tags_href_dashboard = soup.select('a[href^="https://eduardocavalcanti.com/an_fundamentalista"]')
href_dashboard = set()
for tag in tags_href_dashboard:
    href_dashboard.add(tag['href'])



if __name__ == '__main__':
    freeze_support()
    p = Pool(4)
    result = p.map(process_and_insert, href_dashboard)