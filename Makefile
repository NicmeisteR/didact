JSONIQ?=zorba

metadata/leaders:
	$(JSONIQ) ./meta/queries/leaders.xq

metadata/maps:
	$(JSONIQ) ./meta/queries/maps.xq

metadata/game_objects:
	$(JSONIQ) ./meta/queries/game_objects.xq

metadata/leader_powers:
	$(JSONIQ) ./meta/queries/leader_powers.xq

metadata/playlists:
	$(JSONIQ) ./meta/queries/playlists.xq
