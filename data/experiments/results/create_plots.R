library(sqldf)
library(ggplot2)

parse_correctness.function <- function(file) {
  data <- read.csv(file, header=TRUE)
  data$tp  <- as.integer(as.logical(data$tp))
  data$tn  <- as.integer(as.logical(data$tn))
  data$fp  <- as.integer(as.logical(data$fp))
  data$fn  <- as.integer(as.logical(data$fn))
  data$attribute <- as.factor(data$attribute)
  data$occurrence <- as.factor(data$occurrence)
  d <- sqldf('SELECT type, occurrence, attribute, threshold, SUM(tp) AS TP, SUM(tn) AS TN, SUM(fp) AS FP, SUM(fn) AS FN
             FROM data
             GROUP BY type, occurrence, attribute, threshold')
  d$tot <- d$TP + d$TN + d$FP + d$FN 
  d$accuracy <- (d$TP + d$TN) / (d$TP + d$TN + d$FP + d$FN)
  d$recall <- d$TP / (d$TP + d$FN)
  d$precision <- d$TP / (d$TP + d$FP)
  return(d)
}

plot_accuracy.function <- function(d) {
  p<-ggplot(d, aes(x=threshold,
                   y=accuracy,
                   group=group)) +
    geom_line(aes(color=group)) +
    geom_point(aes(color=group)) +
    xlab("Confidence threshold") +
    ylab("Accuracy") +
    scale_x_continuous(limits=c(0,1), breaks=seq(0,1,0.2)) +
    scale_y_continuous(limits=c(0,1), breaks=seq(0,1,0.2)) +
    theme(legend.justification=c(1,0), legend.position=c(1,0)) +
    labs(color = "Uncertainty levels")
  return(p)
}
plot_recall_precision.function <- function(d) {
  p<-ggplot(d, aes(x=precision,
                   y=recall,
                   group=group)) +
    geom_line(aes(color=group)) +
    geom_point(aes(color=group)) +
    xlab("Precision") +
    ylab("Recall") +
    scale_x_continuous(limits=c(0,1), breaks=seq(0,1,0.2)) +
    scale_y_continuous(limits=c(0,1), breaks=seq(0,1,0.2)) +
    theme(legend.justification=c(0,0), legend.position=c(0,0)) +
    labs(color = "Uncertainty levels")
  return(p)
}
plot_ROC.function <- function(d) {
  d$TP_rate <- d$TP / (d$TP + d$FN)
  d$FP_rate <- d$FP / (d$TN + d$FP)
  p<-ggplot(d, aes(x=FP_rate,
                   y=TP_rate,
                   group=group)) +
    geom_line(aes(color=group)) +
    geom_point(aes(color=group)) +
    xlab("False Positive Rate") +
    ylab("True Positive Rate") +
    scale_x_continuous(limits=c(0,1), breaks=seq(0,1,0.2)) +
    scale_y_continuous(limits=c(0,1), breaks=seq(0,1,0.2)) +
    theme(legend.justification=c(1,0), legend.position=c(1,0)) +
    labs(color = "Uncertainty levels")
  return(p)
}
plot_recall.function <- function(d) {
  p<-ggplot(d, aes(x=threshold,
                   y=recall,
                   group=group)) +
    geom_line(aes(color=group)) +
    geom_point(aes(color=group)) +
    xlab("Confidence threshold") +
    ylab("Recall") +
    scale_x_continuous(limits=c(0,1), breaks=seq(0,1,0.2)) +
    scale_y_continuous(limits=c(0,1), breaks=seq(0,1,0.2)) +
    theme(legend.justification=c(1,1), legend.position=c(1,1)) +
    labs(color = "Uncertainty levels")
  return(p)
}

attribute_correctness.function <- function(){
  d <- parse_correctness.function("./attribute_correctness.csv")
  d$group <- d$attribute
  write.csv(d,file="attribute_correctness_summary.csv")
  p <- plot_accuracy.function(d)
  ggsave("plots/attribute_accuracy.pdf", width = 12, height = 12, units = "cm")
  p <- plot_recall.function(d)
  ggsave("plots/attribute_recall.pdf", width = 12, height = 12, units = "cm")
  p <- plot_recall_precision.function(d)
  ggsave("plots/attribute_RecallPrecision.pdf", width = 12, height = 12, units = "cm")
  p <- plot_ROC.function(d)
  ggsave("plots/attribute_ROC.pdf", width = 12, height = 12, units = "cm")
}
pattern_correctness.function <- function(){
  d <- parse_correctness.function("./pattern_correctness.csv")
  d$group <- d$occurrence
  write.csv(d,file="pattern_correctness_summary.csv")
  p <- plot_accuracy.function(d)
  ggsave("plots/pattern_accuracy.pdf", width = 12, height = 12, units = "cm")
  p <- plot_recall.function(d)
  ggsave("plots/pattern_recall.pdf", width = 12, height = 12, units = "cm")
  p <- plot_recall_precision.function(d)
  ggsave("plots/pattern_RecallPrecision.pdf", width = 12, height = 12, units = "cm")
  p <- plot_ROC.function(d)
  ggsave("plots/pattern_ROC.pdf", width = 12, height = 12, units = "cm")
}
combined_correctness.function <- function(){
  d <- parse_correctness.function("./combined_correctness.csv")
  head(d)
  d$group <- d$attribute
  write.csv(d,file="combined_correctness_summary.csv")
  p <- plot_accuracy.function(d)
  ggsave("plots/combined_accuracy.pdf", width = 12, height = 12, units = "cm")
  p <- plot_recall.function(d)
  ggsave("plots/combined_recall.pdf", width = 12, height = 12, units = "cm")
  p <- plot_recall_precision.function(d)
  ggsave("plots/combined_RecallPrecision.pdf", width = 12, height = 12, units = "cm")
  p <- plot_ROC.function(d)
  ggsave("plots/combined_ROC.pdf", width = 12, height = 12, units = "cm")
}

# Generate "correctness" plots
attribute_correctness.function()
pattern_correctness.function()
combined_correctness.function()

data_summary.function <- function(data, varname, groupnames){
  require(plyr)
  summary_func <- function(x, col){
    c(mean = mean(x[[col]], na.rm=TRUE),
      sd = sd(x[[col]], na.rm=TRUE))
  }
  data_sum<-ddply(data, groupnames, .fun=summary_func,
                  varname)
  data_sum <- rename(data_sum, c("mean" = varname))
  return(data_sum)
}
parse_performance.function <- function(file) {
  d <- read.csv(file, header=TRUE)
  d
  d <- data_summary.function(d, varname="exec_time", groupnames=c("rate", "threshold", "type", "ratio"))
  return(d)
}

plot_performance.function <- function(d){
  ggplot(d, aes(x=rate, y=exec_time, group=type, color=type)) +
    #geom_errorbar(aes(ymin=exec_time-sd, ymax=exec_time+sd), width=.1) +
    geom_line() + geom_point() +
    xlab("Stream Rate (events/second)") +
    ylab("Query Execution Time (ms)") +
    scale_x_continuous(limits=c(0,1000), breaks=seq(0,1000,100)) +
    scale_y_continuous(limits=c(0,300), breaks=seq(0,1000,100)) +
    theme(legend.justification=c(0,1), legend.position=c(0,1)) +
    theme(legend.title = element_blank())
}

# Generate "performance" plots
d0 <- parse_performance.function("./baseline_performance.csv")
write.csv(d0,file="attribute_baseline_summary.csv")
p0 <- plot_performance.function(d0)
ggsave("plots/baseline_performance.pdf", width = 12, height = 12, units = "cm")

d1 <- parse_performance.function("./attribute_performance.csv")
write.csv(d1,file="attribute_performance_summary.csv")
plot_performance.function(d1)
ggsave("plots/attribute_performance.pdf", width = 12, height = 12, units = "cm")

d2 <- parse_performance.function("./pattern_performance.csv")
write.csv(d2,file="pattern_performance_summary.csv")
plot_performance.function(d2)
ggsave("plots/pattern_performance.pdf", width = 12, height = 12, units = "cm")

d3 <- parse_performance.function("./combined_performance.csv")
write.csv(d3,file="combined_performance_summary.csv")
plot_performance.function(d)
ggsave("plots/combined_performance.pdf", width = 12, height = 12, units = "cm")

d_all <- rbind(d0, d1, d2, d3)
d_all
plot_performance.function(d_all)
