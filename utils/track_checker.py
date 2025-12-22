import os
import json
from utils.logger import log_info

def check_downloaded_files(output_dir, tracks):
    downloaded = []
    pending = []
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
            with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"utils/track_checker.py:14","message":"created output_dir","data":{"output_dir":output_dir},"timestamp":int(__import__('time').time()*1000)}) + '\n')
        existing_files = set(os.listdir(output_dir))
    except Exception as e:
        existing_files = set()  # Return empty set if directory doesn't exist or can't be read

    for track in tracks:
        filename = f"{track['artist']} - {track['track']}.mp3".replace("/", "-")
        if filename in existing_files:
            downloaded.append(track)
        else:
            pending.append(track)

    log_info(f"Downloaded: {len(downloaded)} tracks, Pending: {len(pending)} tracks")
    return len(downloaded), pending


"""
    Checks which playlists and tracks have already been downloaded.
    Returns:
        downloaded_playlists: list of dicts with playlist info and downloaded tracks
        pending_playlists: list of dicts with playlist info and pending tracks
    """
def check_downloaded_playlists(output_dir, playlists):
    # #region agent log
    with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"utils/track_checker.py:26","message":"check_downloaded_playlists entry","data":{"output_dir":output_dir,"playlists_count":len(playlists) if playlists else 0},"timestamp":int(__import__('time').time()*1000)}) + '\n')
    # #endregion
    downloaded_playlists = []
    pending_playlists = []

    for pl in playlists:
        # #region agent log
        with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
            f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"utils/track_checker.py:32","message":"processing playlist","data":{"playlist_name":pl.get("name","unknown"),"has_items":bool(pl.get("items")),"has_tracks":bool(pl.get("tracks")),"pl_keys":list(pl.keys())},"timestamp":int(__import__('time').time()*1000)}) + '\n')
        # #endregion
        playlist_name = pl["name"]
        sanitized_name = playlist_name.replace("/", "-").strip()
        playlist_dir = os.path.join(output_dir, sanitized_name)

        # #region agent log
        try:
            items = pl.get("items", [])
            tracks_direct = pl.get("tracks", [])
            with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"utils/track_checker.py:36","message":"before track extraction","data":{"has_items":bool(items),"has_tracks_direct":bool(tracks_direct),"items_count":len(items) if items else 0,"tracks_count":len(tracks_direct) if tracks_direct else 0},"timestamp":int(__import__('time').time()*1000)}) + '\n')
        except Exception as e:
            with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"utils/track_checker.py:36","message":"error accessing playlist structure","data":{"error":str(e),"error_type":type(e).__name__},"timestamp":int(__import__('time').time()*1000)}) + '\n')
        # #endregion

        # #region agent log
        try:
            # Check if playlist has direct "tracks" array (Exportify format)
            if pl.get("tracks") and isinstance(pl.get("tracks"), list) and len(pl.get("tracks", [])) > 0:
                first_track = pl.get("tracks")[0]
                if isinstance(first_track, dict) and "artist" in first_track and "track" in first_track:
                    tracks = pl.get("tracks", [])
                    with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"utils/track_checker.py:70","message":"using direct tracks structure","data":{"tracks_count":len(tracks)},"timestamp":int(__import__('time').time()*1000)}) + '\n')
                else:
                    # Try items structure
                    tracks = [
                        {
                            "artist": item["track"]["artistName"],
                            "track": item["track"]["trackName"]
                        }
                        for item in pl.get("items", [])
                        if item.get("track")
                    ]
                    with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                        f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"utils/track_checker.py:82","message":"using items structure","data":{"tracks_count":len(tracks)},"timestamp":int(__import__('time').time()*1000)}) + '\n')
            else:
                # Use items structure (Spotify export format)
                tracks = [
                    {
                        "artist": item["track"]["artistName"],
                        "track": item["track"]["trackName"]
                    }
                    for item in pl.get("items", [])
                    if item.get("track")
                ]
                with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                    f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"utils/track_checker.py:95","message":"using items structure (no tracks key)","data":{"tracks_count":len(tracks)},"timestamp":int(__import__('time').time()*1000)}) + '\n')
        except KeyError as e:
            with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"A","location":"utils/track_checker.py:97","message":"KeyError during track extraction","data":{"error":str(e),"error_type":type(e).__name__,"pl_structure":str(pl)[:200]},"timestamp":int(__import__('time').time()*1000)}) + '\n')
            raise
        except Exception as e:
            with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"E","location":"utils/track_checker.py:100","message":"unexpected error during track extraction","data":{"error":str(e),"error_type":type(e).__name__},"timestamp":int(__import__('time').time()*1000)}) + '\n')
            raise
        # #endregion

        if not os.path.exists(playlist_dir):
            log_info(f"Playlist folder missing: {playlist_name}")
            pending_playlists.append({
                "name": playlist_name,
                "tracks": tracks
            })
            continue

        # #region agent log
        try:
            existing_files = set(os.listdir(playlist_dir))
            with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"utils/track_checker.py:103","message":"os.listdir succeeded for playlist","data":{"playlist_dir":playlist_dir,"file_count":len(existing_files)},"timestamp":int(__import__('time').time()*1000)}) + '\n')
        except Exception as e:
            with open('/Users/saadaboussabr/Desktop/Archive/Temp/spotify-yt-dlp-downloader/.cursor/debug.log', 'a') as f:
                f.write(json.dumps({"sessionId":"debug-session","runId":"run1","hypothesisId":"B","location":"utils/track_checker.py:103","message":"os.listdir failed for playlist","data":{"playlist_dir":playlist_dir,"error":str(e),"error_type":type(e).__name__},"timestamp":int(__import__('time').time()*1000)}) + '\n')
            raise
        # #endregion
        downloaded_tracks = []
        pending_tracks = []

        for track in tracks:
            filename = f"{track['artist']} - {track['track']}.mp3".replace("/", "-")
            if filename in existing_files:
                downloaded_tracks.append(track)
            else:
                pending_tracks.append(track)

        log_info(f"{playlist_name} → Downloaded: {len(downloaded_tracks)}, Pending: {len(pending_tracks)}")

        if pending_tracks:
            pending_playlists.append({
                "name": playlist_name,
                "tracks": pending_tracks
            })
        else:
            downloaded_playlists.append({
                "name": playlist_name,
                "tracks": downloaded_tracks
            })

    return downloaded_playlists, pending_playlists