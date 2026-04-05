from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

output = Path(r'F:\Trading\reports\vinayak_production_scorecard_executive_2026-04-04.pdf')
output.parent.mkdir(parents=True, exist_ok=True)

lines = [
    'Vinayak Executive Production Scorecard',
    'Snapshot Date: 2026-04-04',
    '',
    'Overall Score: 8.3 / 10',
    'Verdict: Ready for controlled production-style rollout.',
    'Not recommended for blind unrestricted launch yet.',
    '',
    'Category Scores',
    'Architecture: 9.0/10',
    'Security and Auth: 8.8/10',
    'Execution Safety: 8.8/10',
    'Data and Persistence: 8.0/10',
    'Observability: 8.4/10',
    'Web Launch Readiness: 8.2/10',
    'Operations and Deployment: 7.8/10',
    '',
    'What Is Strong',
    '- Clean Vinayak app structure and web/app/data separation path.',
    '- Hardened auth and session flow compared with earlier project state.',
    '- Execution path is safer with approval gating and duplicate protection.',
    '- Worker notification path is fixed and no longer leaks Telegram secrets.',
    '- Unit suite is passing cleanly.',
    '',
    'What Still Limits Full Production Confidence',
    '- HTTPS, secret management, and final host-level deployment validation still matter.',
    '- Backup and restore drills are still operational work, not just code work.',
    '- Repeated soak testing is still needed before broader live use.',
    '',
    'Recommended Next Move',
    'Launch with the manual 3-EC2 model:',
    '- Web EC2 for nginx',
    '- App EC2 for API and workers',
    '- Data EC2 for PostgreSQL, Redis, and RabbitMQ',
    '',
    'Promotion Rule',
    'Promote from controlled rollout to broader production use only after repeated smoke tests,',
    'backup validation, and stable live operations evidence.',
]

page_width, page_height = A4
left = 18 * mm
right = 18 * mm
usable_width = page_width - left - right
line_height = 6 * mm
font_name = 'Helvetica'
font_size = 11

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
pdf.setTitle('Vinayak Executive Production Scorecard')

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
