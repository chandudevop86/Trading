from pathlib import Path
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.pdfbase.pdfmetrics import stringWidth
from reportlab.pdfgen import canvas

output = Path(r'F:\Trading\reports\vinayak_production_scorecard.pdf')
output.parent.mkdir(parents=True, exist_ok=True)

lines = [
    'Vinayak Production Scorecard',
    'Snapshot Date: 2026-04-02',
    '',
    'Updated Ratings',
    'Architecture: 9.0/10 - Strong native Vinayak app structure with migrated workflow seams.',
    'Execution Safety: 8.5/10 - Single execution path, reviewed-trade gate, DB constraints, caps, kill switch.',
    'Data Layer: 7.5/10 - Stronger preparation and validation, but still room for broader operational hardening.',
    'Strategy Engine: 8.0/10 - Native strategy workflow and strict signal contract are in place.',
    'UI/UX: 7.5/10 - Admin and user roles are split, with DB-backed auth and cleaner views.',
    'Observability: 8.0/10 - Metrics, stage visibility, alerts, logs, and dashboard are implemented.',
    'Production Readiness: 7.5/10 - Paper-trading capable, not yet unrestricted live-trading ready.',
    '',
    'Current Verdict',
    'Paper-first / controlled rollout.',
    'Not yet ready for unrestricted live trading.',
    '',
    'What Improved Since The Older Scorecard',
    '- Vinayak is now the main native project path.',
    '- Legacy strategy and service seams were migrated out of core workflow paths.',
    '- Execution now goes through ExecutionService.create_execution only.',
    '- Reviewed-trade approval is required before real execution.',
    '- Duplicate prevention has Python and DB enforcement.',
    '- Portfolio caps and kill switch are in place.',
    '- Full native metrics engine is implemented.',
    '- Observability dashboard now has stronger stage and metric accuracy.',
    '- Admin/user auth is DB-backed with role-aware web access.',
    '',
    'Why It Is Still Not Fully Live Ready',
    '- Code quality is stronger than before, but live readiness requires real paper-trading evidence.',
    '- Broker-side operational behavior still needs longer soak testing under realistic conditions.',
    '- Deployment, backup, alerting, and failure-recovery procedures still need continued field validation.',
    '',
    'Recommended Next Steps',
    '1. Continue paper trading and collect a larger clean metrics sample.',
    '2. Review execution logs and blocked-trade metrics daily.',
    '3. Add live broker dry-run checks and operational drills.',
    '4. Keep deployment backups, restart checks, and monitoring active.',
    '5. Promote to limited live mode only after metrics and operations stay stable.',
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
