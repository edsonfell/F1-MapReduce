# Contabilizador de vitórias na F1 por equipe

Utilizando o [Dataset](https://www.kaggle.com/datasets/rohanrao/formula-1-world-championship-1950-2020)  Kaggle, o programa calcula a quantidade de vitórias por equipe de Formula 1 entre 1950 e 2023 (última atualização do dataset).

Para realizar esse calculo o programa utiliza MapReduce no PySpark e utiliza algumas funcionalidades do Pandas para gerenciar os dataframes e suas colunas.

O programa foi construído como um exercício de uma das disciplinas da minha pós-graduação de Big Data no Senac em 2023.



# Para executar o programa
É necessário ter uma versão atualizada do Python3 e versões compatíveis da biblioteca PySpark com a sua instalação do Spark em si. No meu caso utilizei o Spark 3.4 junto com a lib PySpark 3.4 também, além do Pandas.
Basta rodar a função ***main()*** do arquivo main.py

## Diretórios e arquivos
Por hora esse projeto está separado em apenas dois arquivos (main.py e utils.py). No futuro devo dar continuidade para extrair outras informações do dataset e com isso organizar o programa em uma melhor estrutura de arquivos e também os devidos testes unitários.
O diretório **csv_files/** contém apenas os arquivos csv que o programa utiliza, porém contém uma cópia em arquivo .zip de todo o dataset do Kaggle.

## Resultado

O resultado do programa se dá através do arquivo **final_result.csv** que é exportado na pasta raiz do projeto. É possível ver uma previa dele já aqui no repositório.