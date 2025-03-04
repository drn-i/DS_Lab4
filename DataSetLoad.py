import pandas

input_file = 'marketing_sample_for_amazon_com-ecommerce.csv'
output_file = 'prices_data.txt'
dataset = pandas.read_csv(input_file)

with open(output_file, 'w', encoding='utf-8') as f:
    for _, item in dataset.iterrows():
        price = item.get('Selling Price', None)
        if price is not None:
            try:
                price = str(price).replace('$', '').replace(',', '').strip()

                if price.replace('.', '', 1).isdigit():
                    f_price = float(price)
                    f.write(f"{f_price}\n")

            except ValueError:
                pass

print(f"Prices Saved to {output_file}!")
