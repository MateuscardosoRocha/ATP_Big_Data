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
public class Informacao3 {
    
  public static class MapperInformacao3 extends Mapper<Object, Text, Text, IntWritable> {
        
        @Override
        public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
            
            String linha = valor.toString();
            String[] campos = linha.split(";");
            
            if (campos.length == 10) {
                
                String pais = campos[1];
                int ocorrencia = 1;
                
                Text chaveMap = new Text(pais);
                IntWritable valorMap = new IntWritable(ocorrencia);
                
                if (!pais.equals("year")) {
                    context.write(chaveMap, valorMap);
                }
                
            }
            
        }
        
    }
    
    public static class ReducerInformacao3 extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context) throws IOException, InterruptedException {
            
            int soma = 0;
            
            for (IntWritable valor: valores){
                soma += valor.get();
            }

            IntWritable valorSaida = new IntWritable(soma);
            context.write(chave, valorSaida);
        }
        
    }
    
    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/gilson.edivaldo/Desktop/atp/informacao3";
        
        if (args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }
        
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao3");
        
        job.setJarByClass(Informacao3.class);
        job.setMapperClass(MapperInformacao3.class);
        job.setReducerClass(ReducerInformacao3.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        
        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));
        
        job.waitForCompletion(true);
    }
    
}

