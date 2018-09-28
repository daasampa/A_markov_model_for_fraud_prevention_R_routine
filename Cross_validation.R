##########Prueba de validacion cruzada.
##########Versión: 1.0. (control de versiones disponible en: 
##########https://github.com/daasampa)##########
##########Autor: David Arango Sampayo.##########
##########Area: Gerencia Analitica de Gestion del Fraude. 
##########Direccion de Gestion del Fraude.##########

#Funcion para simular trayectorias por cada uno de los clientes en el
#conjunto de estimacion del modelo.
validacion_cruzada = function(i, nsims, horizonte, logico){
  estados_reales = rowSums(matrices_transicion[[i]]) > 0
  
  semi_P = matrices_transicion[[i]][labels(estados_reales[estados_reales]), 
                                    labels(estados_reales[estados_reales])]
  
  if(sum(rowSums(semi_P) == 0) == 0){
    P = t(sapply(seq(1, length(rowSums(semi_P))), function(i){semi_P[i, ] / rowSums(semi_P)[i]}))
    
    simulador_MC = new("markovchain", 
                       states = labels(estados_reales[estados_reales]), 
                       byrow = TRUE, 
                       transitionMatrix = P)
    
    tau = replicate(nsims, min(which(rmarkovchain(n = horizonte, 
                                                  object = simulador_MC, 
                                                  t0 = "Cero", 
                                                  include.t0 = TRUE) == "Fraude")))
    
    if(length(unique(tau)) > 2){
      tau = tau[is.finite(tau)]
      tau = data.frame(tau)
      tau_real = min(which(cadenas_clientes[[i]][, "cadena"] == "Fraude"))
      
      densidad = density(tau[, "tau"])
      
      maxima_densidad = densidad$x[which(densidad$y == max(densidad$y))]
      
      medida =  (abs(maxima_densidad - tau_real) / (max(densidad$x) - min(densidad$x))) * 100
    }
    
    densidad_cliente = ggplot(tau, aes(x = tau)) + 
      geom_density(color = "blue", fill = "lightblue", alpha = 0.5) + 
      geom_vline(aes(xintercept = tau_real, linetype = "dashed")) +
      geom_vline(aes(xintercept = maxima_densidad, color = "orange", linetype = "dashed")) +
      theme(legend.position="none") + 
      labs(title = "", x = "Tiempo primer arribo", y = "Densidad estimada")
  }else{
    medida = NULL
  }
  
  if(logico == TRUE){
    return(list(medida, ggplotly(densidad_cliente)))
  }else{
    return(medida)
  }
}

indices_matrices = which(sapply(seq(1, length(matrices_transicion)), 
                                function(j){sum(colnames(matrices_transicion[[j]]) == 'Fraude') > 0}))

indicadores_validacion = sapply(indices_matrices, 
                                function(j){validacion_cruzada(j, 5000, 1000, FALSE)
                                })
100 - quantile(indicadores_validacion, c(0.75, 0.50))
