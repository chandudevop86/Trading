from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

output = Path(r'F:\Trading\reports\vinayak_manual_3ec2_launch_guide.pdf')
output.parent.mkdir(parents=True, exist_ok=True)

lines = [
    'Vinayak Manual 3-EC2 Production Launch Guide',
    '',
    'Tier Layout',
    '1. Web EC2: nginx only, public HTTPS entrypoint, reverse proxy layer.',
    '2. App EC2: Vinayak API, outbox worker, queue worker.',
    '3. Data EC2: PostgreSQL, Redis, RabbitMQ.',
    '',
    'Network Flow',
    'Browser -> Web EC2 nginx -> App EC2 FastAPI -> Data EC2 services',
    'Use private IP traffic between App EC2 and Data EC2 only.',
    '',
    'Security Groups',
    'Web EC2: allow 80 and 443 from internet, 22 from admin IP only.',
    'App EC2: allow 8000 only from Web EC2, 22 from admin IP only.',
    'Data EC2: allow 5432, 6379, 5672 only from App EC2, 22 from admin IP only.',
    '',
    'Data EC2 Manual Setup',
    '- Install PostgreSQL, Redis, RabbitMQ manually.',
    '- Create PostgreSQL database vinayak and user vinayak with strong password.',
    '- Bind all stateful services to private interfaces only.',
    '- Create a dedicated RabbitMQ user instead of relying on guest.',
    '',
    'App EC2 Manual Setup',
    '- Install Python 3.12, pip, venv, git, and build tools.',
    '- Clone the repo into /opt/vinayak or another stable path.',
    '- Create a venv and install dependencies from requirements.txt.',
    '- Build a real env file from prod.env.example.',
    '- Point VINAYAK_DATABASE_URL to the Data EC2 private IP on 5432.',
    '- Point REDIS_URL to the Data EC2 private IP on 6379.',
    '- Point MESSAGE_BUS_URL to the Data EC2 private IP on 5672.',
    '- Set VINAYAK_ADMIN_USERNAME, VINAYAK_ADMIN_PASSWORD, VINAYAK_ADMIN_SECRET.',
    '- Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID if notifications are needed.',
    '- Set DHAN_CLIENT_ID and DHAN_ACCESS_TOKEN if broker integration is needed.',
    '- Keep VINAYAK_SECURE_COOKIES=true.',
    '- Run Alembic migrations before opening traffic.',
    '- Start the API, outbox worker, and queue worker manually first.',
    '',
    'Web EC2 Manual Setup',
    '- Install nginx only.',
    '- Update nginx proxy_pass to the App EC2 private IP on port 8000.',
    '- Example upstream: http://10.0.2.15:8000',
    '- Enable HTTPS before public launch.',
    '',
    'Manual Validation Sequence',
    '1. Confirm PostgreSQL, Redis, and RabbitMQ are reachable from App EC2.',
    '2. Confirm App EC2 passes /health, /health/live, and /health/ready.',
    '3. Confirm Web EC2 nginx can proxy to the App EC2 private IP.',
    '4. Browser smoke test /login, /admin, /workspace, /dashboard/live-analysis.',
    '5. Test worker-driven Telegram notifications if enabled.',
    '',
    'Recommended Maturity Path',
    '1. Manual install and process understanding',
    '2. systemd services',
    '3. Backups and logging discipline',
    '4. Shell automation',
    '5. Ansible',
    '6. Terraform',
    '7. Docker',
    '8. Kubernetes',
    '',
    'Summary',
    'The cleanest manual production-first Vinayak launch today is:',
    '- one Web EC2 for nginx',
    '- one App EC2 for API and workers',
    '- one Data EC2 for PostgreSQL, Redis, and RabbitMQ',
]

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

pdf = canvas.Canvas(str(output), pagesize=A4)
pdf.setTitle('Vinayak Manual 3-EC2 Production Launch Guide')

x = left
y = page_height - 20 * mm
pdf.setFont(font_name, font_size)

for raw in lines:
    wrapped = wrap(raw)
    for line in wrapped:
        if y < 20 * mm:
            pdf.showPage()
            pdf.setFont(font_name, font_size)
            y = page_height - 20 * mm
        pdf.drawString(x, y, line)
        y -= line_height

pdf.save()
print(output)
