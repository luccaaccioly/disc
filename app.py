import pygsheets
import datetime
import requests
import logging
from chalice import Chalice, Cron
import json
from collections import defaultdict


DISCORD_WEBHOOK_MARTECHDM = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_MARTECH_FRANCO = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_IPC = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_GETROI = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_COSTABUILDER = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_WIZI = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_LUCA = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_TOPWEB = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_PONTESWEB = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'
DISCORD_WEBHOOK_FENIX = 'https://discord.com/api/webhooks/1219632251864748136/IEZCKeVYoRViiXS6AF9sTiAQ_66C3m7ZyaKGDw5RJ_UZMAEAv1W_MsPw15VM7NEE00zL'


service_account_file = 'chalicelib/service-account.json'
gc = pygsheets.authorize(service_file=service_account_file)


app = Chalice(app_name='multi-webhook-handler')
app.log.setLevel(logging.DEBUG)


def get_yesterday_data(sheet_name, worksheet_title):
    sh = gc.open(sheet_name)
    wks = sh.worksheet_by_title(worksheet_title)

    yesterday = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%d/%m/%Y')
    column_data = wks.get_col(4, include_tailing_empty=False) 

    try:
        row = next(i + 1 for i, cell in enumerate(column_data) if cell == yesterday)

        roi = wks.get_value((row, 6))  
        total_investment = wks.get_value((row, 8))  
        receita_adx = wks.get_value((row, 7))  

        data = {
            "DATA": yesterday,
            "ROI": roi,
            "Investimento Total": total_investment,
            "ADX APP Guru": receita_adx
        }

        return data

    except StopIteration:
        return "Data de ontem não encontrada na planilha."
    except Exception as e:
        return f"Erro ao acessar os dados da planilha: {e}"


def send_discord_alert_adx(full_message, webhook_url):
    if isinstance(full_message, str):
        content = full_message
    else:
        content = json.dumps(full_message, ensure_ascii=False)
    payload = {"content": content}

    response = requests.post(webhook_url, json=payload)
    if response.status_code == 204:
        return 'Alerta enviado com sucesso para o Discord!'
    else:
        return f'Falha ao enviar alerta para o Discord. Código de status: {response.status_code}, Resposta: {response.text}'

def send_discord_alert_bitbucket(data, webhook_url):

    payload = {"content": data} if isinstance(data, str) else data
    response = requests.post(webhook_url, json=payload)
    if response.status_code == 204:
        return 'Alerta enviado com sucesso para o Discord!'
    else:
        return f'Falha ao enviar alerta para o Discord. Código de status: {response.status_code}, Resposta: {response.text}'


def reports_adx():
    sheets_info = [
        ('Renda e Dinheiro - Controle ADX', DISCORD_WEBHOOK_MARTECHDM, "Renda e Dinheiro"),
        ('Minha PME - Controle Arbitragem ADX', DISCORD_WEBHOOK_MARTECHDM, "Minha PME"),
        ('Fatecno - Controle Arbitragem ADX', DISCORD_WEBHOOK_MARTECHDM, "Fatecno"),
        ('MGNEWS - Controle Arbitragem ADX', DISCORD_WEBHOOK_MARTECHDM, "MG News"),
        ('CS Tips Tech - Controle Arbitragem ADX', DISCORD_WEBHOOK_MARTECHDM, "CS Tips Tech"),
        ('Jobsfaet - Controle ADX', DISCORD_WEBHOOK_MARTECHDM, "Jobsfaet"),

        ('Dicas Perfeitas - Controle Arbitragem ADX', DISCORD_WEBHOOK_MARTECH_FRANCO, "Dicas Perfeitas"),
        ('AvanteSites - Controle Arbitragem ADX', DISCORD_WEBHOOK_MARTECH_FRANCO, "Avante Sites"),
        ('AbutreNews - Controle Arbitragem ADX', DISCORD_WEBHOOK_MARTECH_FRANCO, "Abutre News"),


        ('Recommend Central - Controle Arbitragem ADX', DISCORD_WEBHOOK_IPC, "Recommend Central"),
        ('WiredCabin - Controle Arbitragem ADX', DISCORD_WEBHOOK_IPC, "WiredCabin"),
        ('WHW - Controle ADX', DISCORD_WEBHOOK_IPC, "WHW"),
        ('Ler Materias - Controle ADX', DISCORD_WEBHOOK_IPC, "Ler Materias"),
        ('Lifesture - Controle Arbitragem ADX', DISCORD_WEBHOOK_IPC, "Lifesture"),
        

        ('CanalTecnoTudo - Controle Arbitragem ADX (NEW)', DISCORD_WEBHOOK_GETROI, "Canal TecnoTudo"),
        ('FreelaFinanceGroup - Controle Arbitragem ADX', DISCORD_WEBHOOK_GETROI, "Freela Finance Group"),
        ('MagnificFinance - Controle Arbitragem ADX (NEW)', DISCORD_WEBHOOK_GETROI, "Magnific Finance"),

        ('Click Vagas - Controle ADX', DISCORD_WEBHOOK_COSTABUILDER, "Click Vagas"),
        ('Web Dinheiro - Controle Arbitragem ADX', DISCORD_WEBHOOK_COSTABUILDER, "Web Dinheiro"),
        ('Dicas da Andy - Controle Arbitragem ADX', DISCORD_WEBHOOK_COSTABUILDER, "Dicas da Andy"),
        ('Travel Tour - Controle ADX', DISCORD_WEBHOOK_COSTABUILDER, "Travel Tour"),
        ('Noticias Alagoas - Controle Arbitragem ADX', DISCORD_WEBHOOK_COSTABUILDER, "Noticias Alagoas"),

        ('CartãoDeCreditoCO - Controle Arbitragem ADX', DISCORD_WEBHOOK_LUCA, "Cartão de Crédito CO"),

        ('SolicitarCartãoDeCreditoBR - Controle Arbitragem ADX', DISCORD_WEBHOOK_TOPWEB, "Solicitar Cartão de Crédito BR"),

        ('AbrirConta - Controle Arbitragem ADX', DISCORD_WEBHOOK_PONTESWEB, "Abrir Conta"),

        ('Crediemprestimo - Controle ADX', DISCORD_WEBHOOK_FENIX, "Crediemprestimo")

    ]

    meses_extenso = {
        1: 'Janeiro', 2: 'Fevereiro', 3: 'Março', 4: 'Abril',
        5: 'Maio', 6: 'Junho', 7: 'Julho', 8: 'Agosto',
        9: 'Setembro', 10: 'Outubro', 11: 'Novembro', 12: 'Dezembro'
    }

    grouped_sheets_info = defaultdict(list)
    for sheet_name, webhook_url, report_name in sheets_info:
        grouped_sheets_info[webhook_url].append((sheet_name, report_name))

    for webhook_url, reports in grouped_sheets_info.items():
        full_message = ""
        for sheet_name, report_name in reports:
            yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
            worksheet_title = f"{meses_extenso[yesterday.month]}{yesterday.year}"

            result = get_yesterday_data(sheet_name, worksheet_title)
            if isinstance(result, dict):
                data = (f"__**{report_name}:**__\n"
                        f"```Data: {result.get('DATA')}🗓️\n"
                        f"ROI: {result.get('ROI')}\n"
                        f"Total Investido: {result.get('Investimento Total')}\n"
                        f"Receita ADX: {result.get('ADX APP Guru')}```\n\n")
            else:
                data = f"{report_name}: {result}\n"

            full_message += data

        if full_message:
            send_discord_alert_adx(full_message, webhook_url)

    return "Relatórios enviados com sucesso!"


@app.route('/webhook/bitbucket', methods=['POST'], content_types=['application/json'])
def webhook_bitbucket():
    request = app.current_request
    webhook_event = request.json_body
    app.log.debug(f"Recebeu evento de webhook: {webhook_event}")

    repository_name = webhook_event.get('repository', {}).get('name', 'Unknown')
    webhook_url = 'https://discord.com/api/webhooks/1205218732562055218/oVT4iUWKTCN6004-ebjo-FkAf_VTTQXsz7eIkGiV8GG_LQIKzUEYGQmsIIZQADd4rno8'

    if 'pullrequest' in webhook_event:
        pull_request_title = webhook_event['pullrequest']['title']
        branch_name = webhook_event['pullrequest']['source']['branch']['name']
        author = webhook_event.get('actor', {}).get('nickname', 'Unknown')

        if 'comment' in webhook_event:
            comment = webhook_event['comment']['content']['raw']
            message = f"```Pull Request: '{pull_request_title}' Commented! 💬```"
            message += f"```Dev: {author} 🧑🏼‍💻```"
            message += f"```Comment: {comment}```"

        else:
            state = webhook_event['pullrequest']['state']

            if state == 'OPEN':
                message = f"```Pull Request:  '{pull_request_title}' Opened! 🔓```"
                message += f"```Dev🧑🏼‍💻: {author}```"
                message += f"```👀 Code to review in the Branch: '{branch_name}' ```"

            elif state == 'UPDATED':
                message = f"```Pull Request:  '{pull_request_title}' Updated! 🔄```"
                message += f"``` Dev🧑🏼‍💻: {author} ```"

            elif state == 'MERGED':
                message = f"```Pull Request:  '{pull_request_title}' Merged! ✅```"
                message += f"``` Dev🧑🏼‍💻: {author} ```"

            elif state == 'DECLINED':
                message = f"```Pull Request:  '{pull_request_title}' Declined! ❌```"
                message += f"``` Dev🧑🏼‍💻: {author} ```"

            else:
                message = f"```Estado desconhecido: {state}```"

    else:
        branch_name = webhook_event['push']['changes'][0]['new']['name']
        message = f"```New Push - Branch: {branch_name} 🚀```"
        message += f"```Dev: {webhook_event['actor']['nickname']} 🧑🏼‍💻```"

    data = {
        "content": message, 
        "username": repository_name  
    }

    return send_discord_alert_bitbucket(data, webhook_url)

@app.schedule("cron(53 9 * * ? *)")
def send_reports():
    functions = [reports_adx]
    results = []

    for function in functions:
        try:
            result = function()
            results.append(result)
        except Exception as e:
            app.log.error(f"Erro ao executar {function.__name__}: {e}")
            results.append(f"Erro ao executar {function.__name__}: {str(e)}")

    return {'results': results}