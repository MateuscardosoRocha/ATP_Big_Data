/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.atpone.ap;

import java.io.IOException;
import java.util.ArrayList;
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
public class Informacao8 {
    
    public static class MapperInformacao8 extends Mapper<Object, Text, Text, IntWritable>{
    private int pesoProduto = 0;
    private String mercadoria;
    private String ano;
    
    @Override
    public void map(Object chave, Text valor, Context context) throws IOException, InterruptedException {
        String linha = valor.toString();
        String[] campos = linha.split(";");     
        pesoProduto = 0;
        
        if(campos.length >= 8){
            mercadoria = campos[3].trim();
            ano = campos[1].trim();
            
            try{
                pesoProduto = Integer.parseInt(campos[6]);
            }catch(NumberFormatException e){
                pesoProduto = 0;
            }
            
            Text chaveMap = new Text(ano+";"+mercadoria);
            IntWritable valorMap = new IntWritable(pesoProduto);
            
            context.write(chaveMap, valorMap);
        }
    }
 }
    
    
    public static class ReducerInformacao8 extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable pesoProduto = new IntWritable();
        private Text mercadoria = new Text();
        private Text ano = new Text();
        private ArrayList<String> dados = new ArrayList<>();
        private int peso;
        
        @Override
        public void reduce(Text chave, Iterable<IntWritable> valores, Context context){
            peso = 0;
            
            for(IntWritable val: valores){
                if(val.get() > peso){
                    peso = val.get();
                }
            }
            
            dados.add(chave+";"+peso);
        }
        
        @Override
        protected void cleanup(Reducer.Context context) throws IOException, InterruptedException{
            Text chave = new Text();
            String contem = "";
            
            for(int i = 0; i<dados.size(); i++){
                String vts[] = dados.get(i).split(";");
                pesoProduto.set(0);
                mercadoria.set("");
                ano.set("");
                if(contem.contains(vts[0])){continue;}
                else{
                    if(contem.equals("")){
                        contem = vts[0];
                    }else{
                        contem += ";"+ vts[0];
                    }
                }
                for(int j=0; j<dados.size(); j++){
                    String vtsAux[] = dados.get(j).split(";");
                    if(vtsAux[0].equals(vts[0])){
                        if(Integer.parseInt(vtsAux[2]) > pesoProduto.get()){
                            pesoProduto.set(Integer.parseInt(vtsAux[2]));
                            mercadoria.set(vtsAux[1]);
                            ano.set(vtsAux[0]);
                        }
                    }
                }
                chave.set(ano+" "+mercadoria);
                context.write(chave, pesoProduto);
            }
        }
    }
    
   public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {;
        String arquivoEntrada = "/home/Disciplinas/FundamentosBigData/OperacoesComerciais/base_100_mil.csv";
        String arquivoSaida = "/home2/ead2022/SEM1/gilson.edivaldo/Desktop/atp/informacao8";

        if (args.length == 2) {
            arquivoEntrada = args[0];
            arquivoSaida = args[1];
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "informacao8");

        job.setJarByClass(Informacao8.class);
        job.setMapperClass(MapperInformacao8.class);
        job.setReducerClass(ReducerInformacao8.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(arquivoEntrada));
        FileOutputFormat.setOutputPath(job, new Path(arquivoSaida));

        job.waitForCompletion(true);
    }
}
