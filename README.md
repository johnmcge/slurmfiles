# slurmfiles

Process the slurm .csv logfiles generated by ITS-RC-Ops. Current focus is on understanding memory efficiency - how much memory jobs use in relation to how much was requested from the scheduler in the batch submit script. 

1) Generate detail file: ./joblogs --action memeff
2) Generate summary stats via: ./joblogs --action summarystats --memefffile file-from-step1
3) Package summary stats data into Longleaf Azure Function VS solution, re-deploy to project Azure

In progress but not yet working: generate pend time stats per user
