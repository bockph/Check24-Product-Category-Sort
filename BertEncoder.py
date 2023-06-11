import torch
import torch.nn as nn
from torch.nn.utils.rnn import pad_sequence
from transformers import BertModel
from transformers import BertTokenizer


class BertEncoder(nn.Module):
    def __init__(self, bert_url, max_sentence_length=512):
        super(BertEncoder, self).__init__()
        self.max_sentence_length = max_sentence_length
        self.blue_bert_tokenizer = BertTokenizer.from_pretrained(bert_url)
        self.bert_model = BertModel.from_pretrained(bert_url, output_hidden_states=True)
        self.bert_model.eval()

    def forward(self, sentences, bert_layers=[12],sentence_embeddings = True):
        '''

        :param sentences: list of lists
        :return: embeddings of size (batch_size,num_sents,768)
        '''
        try:
            with torch.no_grad():
                batch_tokens = []
                for sent_list in sentences:
                    tokens = self.blue_bert_tokenizer(sent_list, padding='max_length', truncation=True,
                                                      max_length=self.max_sentence_length, return_tensors="pt")
                    batch_tokens.append((tokens['input_ids'], tokens['attention_mask']))

                (tokens, masks_org) = zip(*batch_tokens)
                tokens = pad_sequence(tokens, batch_first=True, padding_value=0)
                masks = pad_sequence(masks_org, batch_first=True, padding_value=0)
                batch_size = tokens.size()[0]
                num_sents = tokens.size()[1]


                bert_outputs = self.bert_model(
                    tokens.view(batch_size * num_sents, self.max_sentence_length),
                    masks.view(batch_size * num_sents, self.max_sentence_length))
                embeddings = []
                for layer in bert_layers:
                    if sentence_embeddings:
                        sentence_embeds = torch.mean(bert_outputs[2][layer], dim=1)
                    else:
                        sentence_embeds=bert_outputs[2][layer]
                    if sentence_embeddings:

                        embeddings.append(sentence_embeds.view(batch_size, num_sents, 768))
                    else:
                        embeddings.append(sentence_embeds.view(batch_size, num_sents,self.max_sentence_length, 768))
                embeddings = torch.cat(embeddings, dim=-1)




            # (batch_size,num_sents,768)
            return embeddings
        except Exception as e:
            print(e)
            print("forwards failed")

