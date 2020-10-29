#!/usr/bin/env bash

# Report builder for RxJava 1.x usage. RxJava 1.x is not supported library which is in the process of being replaced
# by Spring-Reactor.

PACKAGES=$(find . -type d -name "mantis-*" -maxdepth 1)

rxJavaAll=0
reactorAll=0
printf "%-35s %15s %15s\n" "Package" "RxJava" "Reactor"
for package in ${PACKAGES}; do
    rxJava=$(find $package -name "*.java"|xargs cat|grep "import rx."|wc -l|xargs)
    reactor=$(find $package -name "*.java"|xargs cat|grep "import reactor"|wc -l|xargs)

    rxJavaAll=$(( $rxJavaAll + $rxJava ))
    reactorAll=$(( reactorAll + $reactor ))

    total=$(( $rxJava + $reactor ))
    if [[ ${total} -eq 0 ]]; then
        rxJavaPerc=0
        reactorPerc=0
    else
        rxJavaPerc=$(($rxJava * 100 / $total ))
        reactorPerc=$(($reactor * 100 / $total ))
    fi

    printf "%-35s %15s %15s\n" ${package} "${rxJava}(${rxJavaPerc}%)" "${reactor}(${reactorPerc}%)"
done

total=$(( $rxJavaAll + $reactorAll ))
rxJavaPerc=$(($rxJavaAll * 100 / $total ))
reactorPerc=$(($reactorAll * 100 / $total ))
printf "%-35s %15s %15s\n" "All" "${rxJavaAll}(${rxJavaPerc}%)" "${reactorAll}(${reactorPerc}%)"