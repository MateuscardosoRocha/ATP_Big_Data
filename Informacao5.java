/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atpone.ap;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


/**
 *
 * @author r.cardoso4
 */
public class Informacao5 {
    
    public static class MapperInformacao5 extends Mapper<Object, Text, Text, IntWritable>{
    
    @Override
    public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
        String linha = valor.toString();
        String[] campos = linha.split(";");
        int transacoes = 1;
        
        
        if(campos.length == 10 && campos[1].equals("2016") ){
            String mercadoria = campos[3].trim();
            
            Text chaveMap = new Text(mercadoria);
            IntWritable valorMap = new IntWritable(transacoes);
            
            context.write(chaveMap, valorMap);
            
        }
    }
 }
    
    
    public static class ReducerInformacao5 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final Text paisMaxOcorrencia = new Text();
        private final IntWritable maxOcorrencia = new IntWritable(0);
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context){
            int soma = 0;
            
            for(IntWritable valor: valores){
                soma += valor.get();
            }
            
            if(soma > maxOcorrencia.get()){
                maxOcorrencia.set(soma);
                paisMaxOcorrencia.set(chave);
            }
            
        }
        
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException{
            context.write(new Text("Mercadoria com maior quantidade de transaecoes financeiras em 2016: "), maxOcorrencia);
            context.write(paisMaxOcorrencia, maxOcorrencia);
        }
    }
    
   public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/gilson.edivaldo/Desktop/atp/informacao5";

        if (args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao5");

        job.setJarByClass(Informacao5.class);
        job.setMapperClass(MapperInformacao5.class);
        job.setReducerClass(ReducerInformacao5.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);

    }
    }
