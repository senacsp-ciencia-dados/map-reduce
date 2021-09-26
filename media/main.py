from mrjob.job import MRJob

class CalculadoraMedia(MRJob):
  def mapper(self, _, linha):
    dados = linha.rstrip('\n').split(",")
    valor = int(dados[1])
    yield dados[0], valor

  def combiner(self, chave, valores):
    soma = 0
    contador = 0
    for valor in valores:
      contador += 1
      soma += valor
    yield chave, [soma, contador]

  def reducer(self, chave, somas_contadores):
    soma = 0
    contador = 0
    for soma_contador in somas_contadores:
      contador += soma_contador[1]
      soma += soma_contador[0]
    yield "media " + chave, soma/contador

if __name__ == '__main__':
    CalculadoraMedia.run()