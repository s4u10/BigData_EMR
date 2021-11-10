
# importação das biliotecas
from mrjob.job import MRJob
from mrjob.step import MRStep
import re

# regex para considerar apenas palavras
REGEX_ONLY_WORDS = "[\w']+"

# declaração da classe herdando da classe MRJob
class MRDataMining(MRJob):
  
    # passos de mapeamento e redução
    def steps(self):
        return [
            MRStep(mapper = self.mapper_get_words, reducer = self.reducer_count_words),
        #    MRStep(mapper = self.mapper_make_counts_key, reducer = self.reducer_output_words)
        ]

    # fase de mapeamento 1 com split dos dados
    def mapper_get_words(self, _, line):
        words = re.findall(REGEX_ONLY_WORDS, line)
        for word in words:
            yield word.lower(), 1

    # fase de redução 1 para sumarizar as palavras
    def reducer_count_words(self, word, values):
        yield word, sum(values)

    # fase mapeamento 2 para inverter a ordem de chave e valor
    # agora será quantidade de palavras por palavras
  #  def mapper_make_counts_key(self, word, count):
  #      yield '%04d'%int(count), word

    # fase de redução 2 para ordenar a nossa lista de palavras
 #   def reducer_output_words(self, count, words):
 #       for word in words:
 #           yield count, word

# execução do nosso job
if __name__ == '__main__':
    MRDataMining.run()
