#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <float.h>

#define MAX_LINE_LENGTH 1024

// Índices das colunas do arquivo com as notas
#define TP_PRESENCA_LC 25
#define TP_PRESENCA_MT 26
#define COL_NU_NOTA_CN 31
#define COL_NU_NOTA_CH 32
#define COL_NU_NOTA_LC 33
#define COL_NU_NOTA_MT 34
#define COL_NU_NOTA_REDACAO 50
#define QTD_COLUNAS 76

int main(int argc, char** argv) {
    double start_time, end_time;
    start_time = MPI_Wtime(); // Inicia a contagem do tempo de execução

    MPI_Init(&argc, &argv);

    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);  // Identificador do processo
    MPI_Comm_size(MPI_COMM_WORLD, &size);  // Número total de processos

    // Variáveis locais para calcular maior nota (média), menor nota (média), soma das notas (médias) e contagem de médias
    double local_max_grade = -DBL_MAX;
    double local_min_grade = DBL_MAX;
    double local_sum_grades = 0.0;
    int local_count_grades = 0;
    int local_count_enrolls = 0;

    FILE* file = NULL;
    // Leitura e processamento do arquivo por partes
    char linha[MAX_LINE_LENGTH];

    if (rank == 0) {
        file = fopen("microdados_enem_2023/DADOS/MICRODADOS_ENEM_2023.csv", "r");
        if (!file) {
            perror("Erro ao abrir o arquivo");
            MPI_Finalize();
            return EXIT_FAILURE;
        }
        // Pula a linha do cabeçalho
        fgets(linha, MAX_LINE_LENGTH, file);
    }

    int continuar = 1; // Flag para indicar se o processo deve continuar recebendo linhas

    while (continuar) {
        if (rank == 0) {
            // Processo mestre lê uma linha
            if (fgets(linha, MAX_LINE_LENGTH, file) != NULL) {
                // Incrementa o contador de inscritos apenas no processo mestre, que é o único que lê o arquivo
                local_count_enrolls++;
                // Se houver apenas um processo, o mestre processa todas as linhas
                if (size == 1) {
                    continuar = 1;
                } else {
                    // Envia a linha para o processo apropriado
                    int destino = local_count_enrolls % size;

                    if (destino == 0) {
                        // Mestre processa sua própria linha
                        continuar = 1;
                    } else {
                        // Envia a linha para o processo apropriado
                        MPI_Send(linha, MAX_LINE_LENGTH, MPI_CHAR, destino, 0, MPI_COMM_WORLD);
                        continuar = 1;
                    }
                }
            } else {
                // Envia um sinal de término para os processos filhos
                strcpy(linha, "EOF");
                for (int i = 1; i < size; i++) {
                    MPI_Send(linha, MAX_LINE_LENGTH, MPI_CHAR, i, 0, MPI_COMM_WORLD);
                }
                continuar = 0;
            }
        } else {
            // Processos filhos aguardam a linha ou sinal de término
            MPI_Recv(linha, MAX_LINE_LENGTH, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            if (strcmp(linha, "EOF") == 0) {
                continuar = 0;
            } else {
                continuar = 1;
            }
        }

        if (continuar) {
            // Dividir a linha em campos
            char* campos[QTD_COLUNAS]; // Array para armazenar os campos da linha
            char* token = strtok(linha, ";");
            int col = 0;

            // Separar os campos da linha
            while (token != NULL && col < QTD_COLUNAS) {
                campos[col++] = token;
                token = strtok(NULL, ";");
            }

            // Verifica se o participante estava ausente em alguma prova
            if (strcmp(campos[TP_PRESENCA_LC], "1") != 0 || strcmp(campos[TP_PRESENCA_MT], "1") != 0) {
                continue; // Pula esse participante
            }

            // Extrair as notas de cada coluna específica
            double nota_cn = atof(campos[COL_NU_NOTA_CN]);
            double nota_ch = atof(campos[COL_NU_NOTA_CH]);
            double nota_lc = atof(campos[COL_NU_NOTA_LC]);
            double nota_mt = atof(campos[COL_NU_NOTA_MT]);
            double nota_redacao = atof(campos[COL_NU_NOTA_REDACAO]);

            // Verifica se alguma nota é zero (ausente)
            // Não comparo a redação pois a nota da redação é a única que realmente pode ser 0
            if (nota_cn == 0 || nota_ch == 0 || nota_lc == 0 || nota_mt == 0) {
                continue; // Pula esse participante
            }

            // Calcula a média das cinco notas
            double media = (nota_cn + nota_ch + nota_lc + nota_mt + nota_redacao) / 5.0;

            // Atualiza máximos, mínimos, soma e contagem locais
            if (media > local_max_grade) local_max_grade = media;
            if (media < local_min_grade) local_min_grade = media;
            local_sum_grades += media;
            local_count_grades++;
        }
    }

    if (rank == 0 && file) fclose(file);

    // Variáveis globais para reduzir os valores em todos os processos
    double global_max_grade, global_min_grade, global_sum_grades;
    int global_count_grades, global_count_enrolls;

    MPI_Reduce(&local_max_grade, &global_max_grade, 1, MPI_DOUBLE, MPI_MAX, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_min_grade, &global_min_grade, 1, MPI_DOUBLE, MPI_MIN, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_sum_grades, &global_sum_grades, 1, MPI_DOUBLE, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_count_grades, &global_count_grades, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);
    MPI_Reduce(&local_count_enrolls, &global_count_enrolls, 1, MPI_INT, MPI_SUM, 0, MPI_COMM_WORLD);

    end_time = MPI_Wtime(); // Finaliza a contagem do tempo de execução

    // O processo mestre calcula e imprime os resultados finais
    if (rank == 0) {
        double media_geral = global_sum_grades / global_count_grades;
        printf("Maior Média: %.2f\n", global_max_grade);
        printf("Menor Média: %.2f\n", global_min_grade);
        printf("Média Geral: %.2f\n", media_geral);
        printf("Quantidade de Inscritos: %d\n", global_count_enrolls);
        printf("Tempo de execução com %d processo(s): %.2f segundos\n", size, (end_time - start_time));
    }

    MPI_Finalize();
    return 0;
}
