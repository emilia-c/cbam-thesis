import deepdoctection as dd 
import pdfplumber

analyzer = dd.get_dd_analyzer()

# **Process a Single PDF**
pdf_path = "C:/Users/Emilia/Documents/Uni Helsinki/Year Three/KONE Thesis/data/extracting ddg urls/data/ddg_downloads/downloads/4_63.pdf"  # Change this to your actual file path

df = analyzer.analyze(path=pdf_path)
df.reset_state() 

# **Extract Results**
doc=iter(df)
page = next(doc)

print(f'attribute: {page.get_attribute_names()}')