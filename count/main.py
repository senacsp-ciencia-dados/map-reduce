from mrjob.job import MRJob
from mrjob.step import MRStep

class ContadorNomes(MRJob):
  def steps(self):
      return [
          MRStep(mapper=self.mapper_quebra_nome),
          MRStep(mapper=self.mapper_filtra_stop_words,
                reducer=self.reducer)
      ]

  def mapper_quebra_nome(self, _, linha):
    nome_minusculo = linha.lower()
    for nome in nome_minusculo.split():
      yield nome, 1

  def mapper_filtra_stop_words(self, chave, contagem):
    if chave not in ["do", "da"]:
      yield chave, contagem

  def reducer(self, chave, valores):
    yield chave, sum(valores)

if __name__ == '__main__':
    ContadorNomes.run()