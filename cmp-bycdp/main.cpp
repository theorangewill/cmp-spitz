/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Caian Benedicto <caian@ggaunicamp.com>
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to
 * deal in the Software without restriction, including without limitation the
 * rights to use, copy, modify, merge, publish, distribute, sublicense,
 * and/or sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
 * THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING
 * FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
 * IN THE SOFTWARE.
 */


// This define enables the C++ wrapping code around the C API, it must be used
// only once per C++ module.
#define SPITZ_ENTRY_POINT

// Spitz serial debug enables a Spitz-compliant main function to allow
// the module to run as an executable for testing purposes.
// #define SPITZ_SERIAL_DEBUG

#include <spitz/spitz.hpp>

#include <iostream>
#include <sstream>
#include <string>
#include <limits>
#include <vector>
#include <cmath>
#include <cstring>
#include <iomanip>
#include <stdio.h>
#include "semblance.h"
#include <unistd.h>


void SetCabecalhoCMP(Traco *traco)
{
    int mx, my;
    mx = (traco->sx + traco->gx) / 2;
    my = (traco->sy + traco->gy) / 2;
    //Offset zerado pois receptores e fonte estao na mesma coordenada
    traco->offset = 0;
    //Fonte e receptores na mesma coordenada
    traco->sx = mx;
    traco->sy = my;
    traco->gx = mx;
    traco->gy = my;
}

void LiberarMemoria(ListaTracos ***lista, int *tamanho)
{
    LiberarMemoriaSU(lista,tamanho);
}


// Parameters should not be stored inside global variables because the
// Spitz interface does not guarantee memory isolation between job
// manager, committer and workers. This can lead to a race condition when
// each thread tries to initialize the parameters from argc and argv.
struct parameters
{
    std::string who;

    float Vini, Vfin, Vint;
    float wind, aph, azimuth;
    std::string arquivo;
    ListaTracos **listaTracos = NULL;
    int tamanhoLista;

    parameters(int argc, const char *argv[], const std::string& who = "") :
        who(who)
    {
        if (argc < 8) {
            std::cerr << "ERRO: ./main <dado sismico> V_INI V_FIN V_INT WIND APH AZIMUTH" << std::endl;
            std::cerr << "\tARQUIVO: arquivo dos tracos sismicos" << std::endl;
            std::cerr << "\tV_INI:   velocidade inicial" << std::endl;
            std::cerr << "\tV_FIN:   velocidade final" << std::endl;
            std::cerr << "\tV_INT:   quantidade de velocidades avaliadas" << std::endl;
            std::cerr << "\tWIND:    janela do semblance" << std::endl;
            std::cerr << "\tAPH:     aperture" << std::endl;
            std::cerr << "\tAZIMUTH: azimuth" << std::endl;
            exit(1);
        }   

        //Leitura dos parametros
        arquivo = argv[1];
        Vini = atof(argv[2]);
        Vfin = atof(argv[3]);
        Vint = atof(argv[4]);
        wind = atof(argv[5]);
        aph = atof(argv[6]);
        azimuth = atof(argv[7]);
    }

    void print()
    {
        std::cout << who << Vini << " " << Vfin << " " << Vint << " " 
                << wind << " " << aph << " " << azimuth << " "
                << arquivo << std::endl;
        std::cout << listaTracos << " tam:" << tamanhoLista << std::endl;
    }
};

/*

// This class coordinates the execution of jobs
class spitz_main : public spitz::spitz_main
{
public:
    int main(int argc, const char* argv[], const spitz::runner& runner)
    {
        spitz::istream result;
        int r;
        int tracos, i;
        char cdpbuffer[6], amostrasbuffer[100];
        char saida[101], saidaEmpilhado[104], saidaSemblance[104], saidaV[104];
        FILE *arquivoEmpilhado, *arquivoSemblance, *arquivoV;
        Traco tracoSemblance, tracoEmpilhado, tracoV;
        parameters p(argc, argv, "[SM] ");

        //Leitura do arquivo
        if(!LeitorArquivoSU(p.arquivo.c_str(), &(p.listaTracos), &p.tamanhoLista, p.aph, p.azimuth)){
            std::cerr << "ERRO NA LEITURA " << p.arquivo.c_str() << std::endl;
            std::cout << p.who << "ERRO NA LEITURA" << std::endl;
            exit(1);
        }

        strncpy(saida,argv[1],strlen(argv[1])-3);
        strncpy(saidaEmpilhado,saida,strlen(argv[1])-3);
        strcat(saidaEmpilhado,"-empilhado.out.su");
        arquivoEmpilhado = fopen(saidaEmpilhado,"w");
        strncpy(saidaSemblance,saida,strlen(argv[1])-3);
        strcat(saidaSemblance,"-semblance.out.su");
        arquivoSemblance = fopen(saidaSemblance,"w");
        strncpy(saidaV,saida,strlen(argv[1])-3);
        strcat(saidaV,"-V.out.su");
        arquivoV = fopen(saidaV,"w");

        const char *argvjob[] = { argv[0], argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7] };
        int argcjob = 8;
        r = runner.run(argcjob, argvjob, result);
        if (r != 0) {
            std::cerr << "[SM] The execution of the job failed with code " << r << "!" << std::endl;
            exit(1);
        }

        for(tracos=0; tracos<p.tamanhoLista; tracos++){

            std::cout << "\t" << tracos << "[" << p.listaTracos[tracos]->tamanho << "] (cdp= " << p.listaTracos[tracos]->cdp << ") de " << p.tamanhoLista << std::endl;


            memcpy(&tracoEmpilhado,p.listaTracos[tracos]->tracos[0], SEISMIC_UNIX_HEADER);
            SetCabecalhoCMP(&tracoEmpilhado);
            memcpy(&tracoSemblance,&tracoEmpilhado, SEISMIC_UNIX_HEADER);
            memcpy(&tracoV,&tracoEmpilhado, SEISMIC_UNIX_HEADER);

            const char *argvjob[] = { argv[0], argv[1], argv[2], argv[3], argv[4], argv[5], argv[6], argv[7], cdpbuffer, amostrasbuffer };
            int argcjob = 10;
            r = runner.run(argcjob, argvjob, result);
            if (r != 0) {
                std::cerr << "[SM] The execution of the job failed with code " << r << "!" << std::endl;
                exit(1);
            }

            tracoEmpilhado.dados = (float*) malloc(sizeof(float)*p.listaTracos[tracos]->tracos[0]->ns);
            tracoSemblance.dados = (float*) malloc(sizeof(float)*p.listaTracos[tracos]->tracos[0]->ns);
            tracoV.dados = (float*) malloc(sizeof(float)*p.listaTracos[tracos]->tracos[0]->ns);
            for(i=0; i<p.listaTracos[tracos]->tracos[0]->ns; i++)
            {
                result >> tracoEmpilhado.dados[i];
                result >> tracoSemblance.dados[i];
                result >> tracoV.dados[i];
                //if(i%200 == 0) std::cout << ">>" << i << " " << tracoEmpilhado.dados[i] << " " << tracoSemblance.dados[i] << " " << tracoV.dados[i] << std::endl;
            }

            std::cout << "CDP: " << tracoEmpilhado.cdp << " " << tracoSemblance.cdp << " " << tracoV.cdp << std::endl;


            //Copiar os tracos resultantes nos arquivos de saida
            fwrite(&tracoEmpilhado,SEISMIC_UNIX_HEADER,1,arquivoEmpilhado);
            fwrite(&(tracoEmpilhado.dados[0]),sizeof(float),tracoEmpilhado.ns,arquivoEmpilhado);
            fwrite(&tracoSemblance,SEISMIC_UNIX_HEADER,1,arquivoSemblance);
            fwrite(&(tracoSemblance.dados[0]),sizeof(float),tracoSemblance.ns,arquivoSemblance);
            fwrite(&tracoV,SEISMIC_UNIX_HEADER,1,arquivoV);
            fwrite(&(tracoV.dados[0]),sizeof(float),tracoV.ns,arquivoV);

            //Liberar memoria alocada nos dados do traco resultante
            free(tracoEmpilhado.dados);
            free(tracoSemblance.dados);
            free(tracoV.dados);
        }


        fclose(arquivoEmpilhado);
        fclose(arquivoSemblance);
        fclose(arquivoV);

        LiberarMemoria(&(p.listaTracos), &(p.tamanhoLista));

        std::cout << "SALVO NOS ARQUIVOS:\n\t" << saidaEmpilhado << "\n\t" << saidaSemblance << "\n\t" << saidaV << std::endl;

        return 0;
    }
};
*/

// This class creates tasks.
class job_manager : public spitz::job_manager
{
private:
    parameters p;
    int cdp;

public:
    job_manager(int argc, const char *argv[], spitz::istream& jobinfo) :
        p(argc, argv, "[JM] "), cdp(0)
    {
        //Leitura do arquivo
        if(!LeitorArquivoSU(p.arquivo.c_str(), &(p.listaTracos), &p.tamanhoLista, p.aph, p.azimuth)){
            std::cerr << "ERRO NA LEITURA " << p.arquivo.c_str() << std::endl;
            std::cout << p.who << "ERRO NA LEITURA" << std::endl;
            exit(1);
        }
        std::cout << "[JM] Job manager created." << std::endl;
    }

    bool next_task(const spitz::pusher& task)
    {
        spitz::ostream o;
        int i, j;

        if(cdp >= p.tamanhoLista){
            return false;
        }

        o << cdp;
        o << p.listaTracos[cdp]->cdp;
        o << p.listaTracos[cdp]->tamanho;
        o << p.listaTracos[cdp]->tracos[0]->dt;
        o << p.listaTracos[cdp]->tracos[0]->ns;
        for(i=0; i<p.listaTracos[cdp]->tamanho; i++){
            o << p.listaTracos[cdp]->tracos[i]->scalco;
            o << p.listaTracos[cdp]->tracos[i]->sx;
            o << p.listaTracos[cdp]->tracos[i]->sy;
            o << p.listaTracos[cdp]->tracos[i]->gx;
            o << p.listaTracos[cdp]->tracos[i]->gy;
            for(j=0; j<p.listaTracos[cdp]->tracos[i]->ns; j++)
                o << p.listaTracos[cdp]->tracos[i]->dados[j];
        }

        std::cout << p.who << "Generated task for CDP: "<< cdp << "[" << p.listaTracos[cdp]->tamanho << "] (cdp= " << p.listaTracos[cdp]->cdp << ") de " << p.tamanhoLista << std::endl;

        cdp += 1;

        task.push(o);
        return true;
    }

    ~job_manager()
    {
        std::cout << "[JM] Job manager destroyed." <<std::endl;
        LiberarMemoria(&(p.listaTracos), &(p.tamanhoLista));
    }
};

// This class executes tasks, preferably without modifying its internal state
// because this can lead to a break of idempotence between tasks. The 'run'
// method will not impose a 'const' behavior to allow libraries that rely
// on changing its internal state, for instance, OpenCL (see clpi example).
class worker : public spitz::worker
{
private:
    parameters p;
    TracosCDP **tracos;
    int tamanho, namostras;
    int cdp, ncdp;

public:
    worker(int argc, const char *argv[]) : p(argc, argv, "[WK] ")
    {
        //p.print();
        std::cout << "[WK] Worker created." << argc << std::endl;
    }

    int run(spitz::istream& task, const spitz::pusher& result)
    {
        spitz::ostream o;
        int amostra, batch;
        int i, total, a;
        float *Vvector, *Cvector;
        float seg, t0, Vinc, s, bestS, bestV, pilha, pilhaTemp;
        short int dt, ns;
        int w;
        int j;
        
        //Calculo de V e C para a busca
        Vinc = (p.Vfin-p.Vini)/(p.Vint);
        Vvector = (float*) malloc(sizeof(float)*(p.Vint));
        Cvector = (float*) malloc(sizeof(float)*(p.Vint));
        for(i=0; i<p.Vint; i++){
            Vvector[i] = Vinc*i+p.Vini;
            Cvector[i] = 4/Vvector[i]*1/Vvector[i];
        }

        task >> ncdp;
        task >> cdp;
        task >> tamanho;
        task >> dt;
        task >> ns;

        //Tempo entre amostras, convertido para segundos
        seg = ((float) dt)/1000000;
        tracos = (TracosCDP**) malloc(sizeof(TracosCDP*)*tamanho);

        for(i=0; i<tamanho; i++){
            tracos[i] = (TracosCDP*) malloc(sizeof(TracosCDP));
            task >> tracos[i]->scalco;
            task >> tracos[i]->sx;
            task >> tracos[i]->sy;
            task >> tracos[i]->gx;
            task >> tracos[i]->gy;
            tracos[i]->ns = ns;
            tracos[i]->dados = (float*) malloc(sizeof(float)*ns);
            for(j=0; j<ns; j++)
                task >> tracos[i]->dados[j];
        }
        std::cout << "WORKING ON CDP " << cdp << std::endl;

        o << ncdp;
        o << cdp;
        for(a=0; a<ns; a++){
            //Calcula o segundo inicial
            t0 = a*seg;

            //Inicializar variaveis antes da busca
            pilha = tracos[0]->dados[a];
            bestS = 0.0;
            bestV = 0.0;

            //Para cada velocidade
            for(i=0; i<p.Vint; i++){
                pilhaTemp = 0;
                //Calcular semblance
                s = SemblanceWorker(tracos,tamanho,0.0,0.0,Cvector[i],t0,p.wind,seg,&pilhaTemp,p.azimuth);
                if(s<0 && s!=-1) {printf("S NEGATIVO\n"); exit(1);}
                if(s>1) {printf("S MAIOR Q UM %.20f\n", s); exit(1);}
                else if(s > bestS){
                    bestS = s;
                    bestV = Vvector[i];
                    pilha = pilhaTemp;
                }
            }

            o << pilha;
            o << bestS;
            o << bestV;
        }
        result.push(o);

        free(Vvector);
        free(Cvector);

        return 0;
    }

    ~worker()
    {
        int i, j;
        for(i=0; i<tamanho; i++){
            for(j=0; j<namostras; j++)
                free(tracos[i]->dados);
            free(tracos[i]);
        }
        free(tracos);
    }
};

// This class is responsible for merging the result of each individual task
// and, if necessary, to produce the final result after all of the task
// results have been received.
class committer : public spitz::committer
{
private:
    parameters p;
    float **semblance, **empilhado, **velocidade;
    int cdp, ns, cdps, ncdp;

public:
    committer(int argc, const char *argv[], spitz::istream& jobinfo) :
        p(argc, argv, "[CO] ")
    {
        int i;
        //Leitura do arquivo
        if(!LeitorArquivoSUCommit(p.arquivo.c_str(), &(p.listaTracos), &p.tamanhoLista, p.aph, p.azimuth, &ns)){
            std::cerr << "ERRO NA LEITURA " << p.arquivo.c_str() << std::endl;
            std::cout << p.who << "ERRO NA LEITURA" << std::endl;
            exit(1);
        }
        cdps = p.tamanhoLista;

        semblance = (float**) malloc(sizeof(float*)*cdps);
        empilhado = (float**) malloc(sizeof(float*)*cdps);
        velocidade = (float**) malloc(sizeof(float*)*cdps);
        for(i=0; i<cdps; i++){
            semblance[i] = (float*) malloc(sizeof(float)*ns);
            empilhado[i] = (float*) malloc(sizeof(float)*ns);
            velocidade[i] = (float*) malloc(sizeof(float)*ns);
        }

        std::cout << "[CO] Committer created." << std::endl;
    }

    int commit_task(spitz::istream& result)
    {        
        int i;
        
        std::cout << "[CO] Committing result " << std::endl;

        // Accumulate each term of the expansion        
        while(result.has_data()) {

            result >> ncdp;
            result >> cdp;
            std::cout << "[CO] Committing result of CDP " << cdp << "(" << ncdp << ")" << std::endl;
            for(i=0; i<ns; i++){
                result >> empilhado[ncdp][i];
                result >> semblance[ncdp][i];
                result >> velocidade[ncdp][i];
            }
        }
        
        return 0;
    }

    int commit_job(const spitz::pusher& final_result)
    {
        int i;
        spitz::ostream o;
        char saida[101], saidaEmpilhado[104], saidaSemblance[104], saidaV[104];
        FILE *arquivoEmpilhado, *arquivoSemblance, *arquivoV;
        Traco tracoSemblance, tracoEmpilhado, tracoV;

        std::cout << "COMMIT JOB" << std::endl;

        strncpy(saida,p.arquivo.c_str(),strlen(p.arquivo.c_str())-3);
        strncpy(saidaEmpilhado,saida,strlen(p.arquivo.c_str())-3);
        strcat(saidaEmpilhado,"-empilhado.out3.su");
        arquivoEmpilhado = fopen(saidaEmpilhado,"w");
        strncpy(saidaSemblance,saida,strlen(p.arquivo.c_str())-3);
        strcat(saidaSemblance,"-semblance.out3.su");
        arquivoSemblance = fopen(saidaSemblance,"w");
        strncpy(saidaV,saida,strlen(p.arquivo.c_str())-3);
        strcat(saidaV,"-V.out3.su");
        arquivoV = fopen(saidaV,"w");


        // Push the result
        for(cdp=0; cdp<cdps; cdp++){            
            memcpy(&tracoEmpilhado,p.listaTracos[cdp]->tracos[0], SEISMIC_UNIX_HEADER);
            SetCabecalhoCMP(&tracoEmpilhado);
            memcpy(&tracoSemblance,&tracoEmpilhado, SEISMIC_UNIX_HEADER);
            memcpy(&tracoV,&tracoEmpilhado, SEISMIC_UNIX_HEADER);

            //Copiar os tracos resultantes nos arquivos de saida
            fwrite(&tracoEmpilhado,SEISMIC_UNIX_HEADER,1,arquivoEmpilhado);
            fwrite((empilhado[cdp]),sizeof(float),ns,arquivoEmpilhado);
            fwrite(&tracoSemblance,SEISMIC_UNIX_HEADER,1,arquivoSemblance);
            fwrite((semblance[cdp]),sizeof(float),ns,arquivoSemblance);
            fwrite(&tracoV,SEISMIC_UNIX_HEADER,1,arquivoV);
            fwrite((velocidade[cdp]),sizeof(float),ns,arquivoV);
        }

        printf("SALVO NOS ARQUIVOS:\n\t%s\n\t%s\n\t%s\n",saidaEmpilhado,saidaSemblance,saidaV);

        final_result.push(NULL, 0);
        return 0;
    }

    ~committer()
    {   
        int i;
        for(i=0; i<cdps ; i++){
            free(semblance[i]);
            free(empilhado[i]);
            free(velocidade[i]);
        }
        free(semblance);
        free(empilhado);
        free(velocidade);
        std::cout << "[CO] Committer destroyed." << std::endl;
    }
};

// The factory binds the user code with the Spitz C++ wrapper code.
class factory : public spitz::factory
{
public:

    spitz::job_manager *create_job_manager(int argc, const char *argv[],
        spitz::istream& jobinfo)
    {
        return new job_manager(argc, argv, jobinfo);
    }

    spitz::worker *create_worker(int argc, const char *argv[])
    {
        return new worker(argc, argv);
    }

    spitz::committer *create_committer(int argc, const char *argv[],
        spitz::istream& jobinfo)
    {
        return new committer(argc, argv, jobinfo);
    }
};

// Creates a factory class.
spitz::factory *spitz_factory = new factory();

