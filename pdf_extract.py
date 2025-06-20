from PyPDF2 import PdfReader, PdfWriter  # You can also use `from pypdf import PdfReader, PdfWriter`

# Set the input and output file paths
input_path = "H:\\Conflict of Interest.pdf"
output_path = "H:\\Conflict of Interest_pages_26_to_29.pdf"

# Open the PDF file
reader = PdfReader(input_path)
writer = PdfWriter()

# Extract pages 26 to 29 (zero-based index: 25 to 28)
for page_num in range(25, 29):
    writer.add_page(reader.pages[page_num])

# Write to a new PDF
with open(output_path, "wb") as output_file:
    writer.write(output_file)

print("Pages 26 to 29 extracted and saved successfully.")