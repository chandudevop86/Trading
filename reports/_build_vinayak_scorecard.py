from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

output = Path(r'F:\Trading\reports\vinayak_production_scorecard_2026-04-04.pdf')
output.parent.mkdir(parents=True, exist_ok=True)

lines = [
    'Vinayak Production Scorecard',
    'Snapshot Date: 2026-04-04',
    '',
    'Current Ratings',
    'Architecture: 9.0/10 - Clear web, app, execution, messaging, and data boundaries with a workable 3-tier launch path.',
    'Security And Auth: 8.8/10 - Legacy admin bypass removed, bootstrap secrets hardened, session invalidation improved, and worker secret flow tightened.',
    'Execution Safety: 8.8/10 - Reviewed-trade gate, duplicate protection, execution contracts, portfolio caps, and kill switch are in place.',
    'Data And Persistence: 8.0/10 - Schema bootstrap, migrations, and execution persistence are solid, but production backups and restore drills remain operational work.',
    'Observability: 8.4/10 - Health/readiness, alerts, metrics, logs, and dashboard coverage are strong for current scope.',
    'Web Launch Readiness: 8.2/10 - Role-aware web flows are working, cookie posture is stronger, and deployment topology is clearer, but final HTTPS and manual smoke validation still matter.',
    'Operations And Deployment: 7.8/10 - EC2, nginx, env, and worker guidance are now much better, but production maturity still depends on real runbooks, backups, and repeated field operations.',
    'Overall Production Readiness: 8.3/10 - Strong staging or controlled production candidate, but not yet a fully mature large-scale platform.',
    '',
    'Current Verdict',
    'Ready for controlled production-style rollout.',
    'Recommended for staged launch, not blind unrestricted launch.',
    '',
    'What Is Strong Right Now',
    '- Full Vinayak unit suite is passing cleanly.',
    '- Admin auth is much safer than the earlier project state.',
    '- Notification worker path is fixed and no longer sends Telegram secrets over the bus.',
    '- Readiness output is safer and less noisy.',
    '- Production and UAT deployment files are closer to a real web/app/data separation model.',
    '- Manual 3-EC2 runbooks and systemd guides now exist for a production-first rollout path.',
    '',
    'What Still Keeps It From A Higher Score',
    '- Public launch still depends on correct HTTPS, DNS, and real secret management in the deployed environment.',
    '- Operational evidence matters: backup testing, restart drills, and longer soak runs are still needed.',
    '- RabbitMQ, Redis, and PostgreSQL are still operator-managed unless moved to stronger managed services later.',
    '- Final production confidence needs repeated browser, worker, and recovery smoke tests on the real hosts.',
    '',
    'Recommended Next Steps',
    '1. Launch in staged production with the 3-EC2 manual topology.',
    '2. Validate nginx -> app -> data connectivity end to end on private networking.',
    '3. Run backup and restore drills for PostgreSQL before public launch.',
    '4. Convert the manual steps into systemd, shell, Ansible, and Terraform in that order.',
    '5. Promote to broader production use only after repeated soak validation and operational checklists stay green.',
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
pdf.setTitle('Vinayak Production Scorecard')

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

