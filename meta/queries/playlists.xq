jsoniq version "1.0";

import module namespace file = "http://expath.org/ns/file";

let $playlists := jn:parse-json(file:read-text("./meta/data/playlists.json"))."ContentItems"

return [
    for $playlist in $playlists[]
    where $playlist."Type" = "HW2Playlist"
    order by $playlist."View"."HW2Playlist"."DisplayInfo"."View"."Common"."PublishedUtc"
    return [
        $playlist."View"."Identity",
        $playlist."View"."HW2Playlist"."Image"."View"."Media"."MediaUrl",
        $playlist."View"."HW2Playlist"."ThumbnailImage"."View"."Media"."MediaUrl",
        $playlist."View"."HW2Playlist"."MaxPartySize",
        $playlist."View"."HW2Playlist"."MinPartySize",
        $playlist."View"."HW2Playlist"."IsTeamGamePlaylist",
        $playlist."View"."HW2Playlist"."StatsClassification",
        $playlist."View"."HW2Playlist"."TargetPlatform",
        $playlist."View"."HW2Playlist"."isCrossplay",
        $playlist."View"."HW2Playlist"."DisplayInfo"."View"."Common"."PublishedUtc",
        $playlist."View"."HW2Playlist"."DisplayInfo"."View"."HW2PlaylistDisplayInfo"."Name"
    ]
]

