jsoniq version "1.0";

import module namespace file = "http://expath.org/ns/file";

let $leader_powers_1 := jn:parse-json(file:read-text("./meta/data/leader_powers_1.json"))."ContentItems"
let $leader_powers_2 := jn:parse-json(file:read-text("./meta/data/leader_powers_2.json"))."ContentItems"
let $leader_powers_3 := jn:parse-json(file:read-text("./meta/data/leader_powers_3.json"))."ContentItems"
let $leader_powers := [$leader_powers_1[], $leader_powers_2[], $leader_powers_3[]]

return [
    for $leader_power in $leader_powers[]
    where $leader_power."Type" = "HW2LeaderPower"
    order by $leader_power."View"."HW2LeaderPower"."ObjectTypeId"
    return [ $leader_power."View"."HW2LeaderPower"."ObjectTypeId", $leader_power."View"."Identity" ]
]
