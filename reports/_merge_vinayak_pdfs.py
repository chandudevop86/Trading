from pathlib import Path
from PyPDF2 import PdfMerger

base = Path(r'F:\Trading\reports')
inputs = [
    base / 'vinayak_linux_deployment_guide.pdf',
    base / 'vinayak_ec2_ssl_ops_guide.pdf',
]
output = base / 'vinayak_deployment_handbook.pdf'
merger = PdfMerger()
for item in inputs:
    merger.append(str(item))
with output.open('wb') as handle:
    merger.write(handle)
merger.close()
print(output)
