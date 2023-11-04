import {Button, Card, CardActions, CardContent, Stack, Typography} from "@mui/material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import {materialTheme as theme} from "../../Theme";
import DownloadIcon from "@mui/icons-material/Download";
import React from "react";
import {selectFileDetails} from "../../state/Root";
import {useSelector} from "react-redux";
import {triggerDownload} from "../../utils/BrowserInteraction";
import {Image} from "theme-ui";


const ImageFileViewer = () => {

    const fileDetails = selectFileDetails(useSelector(state => state));

    const handleDownloadFile = () => {
        triggerDownload(fileDetails.rawPath, fileDetails.name);
    }

    return (
        <Stack alignItems={'center'} padding={2}>
            <Image src={fileDetails.rawPath} sx={{width: '100%', height: '100%'}} alt={fileDetails.name}/>
        </Stack>
    )
}

export default ImageFileViewer;