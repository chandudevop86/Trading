from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

REPORTS_DIR = Path(r'F:\Trading\reports')
REPORTS_DIR.mkdir(parents=True, exist_ok=True)

DOCS = {
    'vinayak_systemd_services_guide.pdf': [
        'Vinayak Systemd Services Guide',
        '',
        'Create three systemd services on the App EC2:',
        '- vinayak-api',
        '- vinayak-outbox-worker',
        '- vinayak-queue-worker',
        '',
        'Common assumptions',
        '- app path: /opt/vinayak',
        '- env file: /etc/vinayak/vinayak.env',
        '- venv path: /opt/vinayak/.venv',
        '',
        'API unit essentials',
        '- WorkingDirectory=/opt/vinayak',
        '- EnvironmentFile=/etc/vinayak/vinayak.env',
        '- ExecStart python -m uvicorn app.main:app --host 0.0.0.0 --port 8000',
        '- Restart=always',
        '',
        'Outbox worker unit essentials',
        '- ExecStart python -m vinayak.workers.outbox_worker',
        '',
        'Queue worker unit essentials',
        '- ExecStart python -m vinayak.workers.event_worker',
        '',
        'Enable and start',
        '- systemctl daemon-reload',
        '- systemctl enable vinayak-api',
        '- systemctl enable vinayak-outbox-worker',
        '- systemctl enable vinayak-queue-worker',
        '- systemctl start each service',
        '',
        'Operational checks',
        '- systemctl status for all three services',
        '- journalctl per service after deploys',
        '- restart services after env changes',
        '- verify /health/ready after every deploy',
    ],
    'vinayak_web_ec2_manual_guide.pdf': [
        'Vinayak Web EC2 Manual Guide',
        '',
        'Responsibilities',
        '- nginx only',
        '- TLS termination',
        '- reverse proxy to the App EC2 private IP',
        '',
        'Install',
        '- nginx',
        '- certbot and python3-certbot-nginx if using direct TLS on the host',
        '',
        'Proxy target',
        '- proxy_pass must point to the App EC2 private IP on port 8000',
        '- example: http://10.0.2.15:8000',
        '',
        'Validation',
        '- nginx -t passes',
        '- /login loads through nginx',
        '- /admin loads through nginx',
        '',
        'Security group',
        '- allow 80 and 443 from internet',
        '- allow 22 only from admin IP',
        '- do not expose app or DB ports on this host',
    ],
    'vinayak_app_ec2_manual_guide.pdf': [
        'Vinayak App EC2 Manual Guide',
        '',
        'Responsibilities',
        '- Vinayak API',
        '- outbox worker',
        '- queue worker',
        '',
        'Install base packages',
        '- Python 3.12',
        '- pip',
        '- venv support',
        '- git',
        '- build tools',
        '',
        'Code path',
        '- deploy repo into /opt/vinayak',
        '',
        'Env file essentials',
        '- VINAYAK_DATABASE_URL -> Data EC2 private IP on 5432',
        '- REDIS_URL -> Data EC2 private IP on 6379',
        '- MESSAGE_BUS_URL -> Data EC2 private IP on 5672',
        '- VINAYAK_ADMIN_USERNAME',
        '- VINAYAK_ADMIN_PASSWORD',
        '- VINAYAK_ADMIN_SECRET',
        '- TELEGRAM_BOT_TOKEN',
        '- TELEGRAM_CHAT_ID',
        '- DHAN_CLIENT_ID',
        '- DHAN_ACCESS_TOKEN',
        '- VINAYAK_SECURE_COOKIES=true',
        '',
        'Validation',
        '- run Alembic upgrade',
        '- verify /health, /health/live, /health/ready',
        '- verify /login and /admin',
        '- verify one live-analysis run',
        '',
        'Security group',
        '- allow 8000 only from Web EC2',
        '- allow 22 only from admin IP',
        '- allow outbound 5432, 6379, 5672 to Data EC2',
    ],
    'vinayak_data_ec2_manual_guide.pdf': [
        'Vinayak Data EC2 Manual Guide',
        '',
        'Responsibilities',
        '- PostgreSQL',
        '- Redis',
        '- RabbitMQ',
        '',
        'PostgreSQL',
        '- create vinayak database',
        '- create vinayak user with strong password',
        '- bind to private interface only',
        '',
        'Redis',
        '- bind to private interface only',
        '- never expose publicly',
        '',
        'RabbitMQ',
        '- create dedicated vinayak user',
        '- do not use guest outside local-only testing',
        '',
        'Ports open only to App EC2',
        '- 5432',
        '- 6379',
        '- 5672',
        '',
        'Operations',
        '- daily PostgreSQL backup',
        '- monitor disk and memory',
        '- keep restore steps documented',
        '- confirm services reachable from App EC2',
    ],
}

page_width, page_height = A4
left = 18 * mm
right = 18 * mm
usable_width = page_width - left - right
line_height = 6 * mm
font_name = 'Helvetica'
font_size = 10

def wrap(text: str):
    if not text:
        return ['']
    words = text.split(' ')
    out = []
    current = ''
    for word in words:
        trial = word if not current else current + ' ' + word
        if stringWidth(trial, font_name, font_size) <= usable_width:
            current = trial
        else:
            if current:
                out.append(current)
            current = word
    if current:
        out.append(current)
    return out

for filename, lines in DOCS.items():
    output = REPORTS_DIR / filename
    pdf = canvas.Canvas(str(output), pagesize=A4)
    pdf.setTitle(lines[0])
    x = left
    y = page_height - 20 * mm
    pdf.setFont(font_name, font_size)
    for raw in lines:
        for line in wrap(raw):
            if y < 20 * mm:
                pdf.showPage()
                pdf.setFont(font_name, font_size)
                y = page_height - 20 * mm
            pdf.drawString(x, y, line)
            y -= line_height
    pdf.save()
    print(output)
