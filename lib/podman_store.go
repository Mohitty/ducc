package lib

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	log "github.com/sirupsen/logrus"
)

var (
	//Podman Additional Image Store inside unpacked.cern.ch
	rootPath = "podmanStore"
	//rootfsDir contains the exploded rootfs of images.
	rootfsDir = "overlay"
	//imageMetadataDir contains the metadata, config and manifest file of images.
	imageMetadataDir = "overlay-images"
	//layerMetadataDir contains the metadata of layers
	layerMetadataDir = "overlay-layers"
)

func (img Image) IngestRootfsIntoPodmanStore(CVMFSRepo, subDirInsideRepo string) (err error) {
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}

	for _, layer := range manifest.Layers {
		layerid := strings.Split(layer.Digest, ":")[1]

		//symlinkPath will contain the rootfs of the corresponding layer in podman store.
		symlinkPath := filepath.Join(rootPath, rootfsDir, layerid, "diff")
		targetPath := filepath.Join(subDirInsideRepo, layerid[:2], layerid, "layerfs")

		err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
		if err != nil {
			LogE(err).Error("Error in creating the symlink for the diff dir")
			return err
		}
	}
	return nil
}

func (img Image) CreateLinkDir(CVMFSRepo, subDirInsideRepo string) (err error) {
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	for _, layer := range manifest.Layers {
		layerid := strings.Split(layer.Digest, ":")[1]

		//generate the link id
		lid := generateID(26)

		//Create link dir
		symlinkPath := filepath.Join(rootPath, rootfsDir, "l", lid)
		targetPath := filepath.Join(rootPath, rootfsDir, layerid, "diff")
		
		err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
		if err != nil {
			LogE(err).Error("Error in creating the symlink for the Link dir")
			return err
		}

		//Create link file
		tmpFile, err := ioutil.TempFile("", "linkfile")
		if err != nil {
			return err
		}

		err = ioutil.WriteFile(tmpFile.Name(), []byte(lid), 0644)
		tmpFile.Close()
		if err != nil {
			LogE(err).Error("Error in writing to the link file")
			return err
		}

		//ingest the link file
		linkPath := filepath.Join(rootPath,"overlay",layerid,"link")
		err = IngestIntoCVMFS(CVMFSRepo, linkPath, tmpFile.Name())
		os.RemoveAll(tmpFile.Name())
		if err != nil {
			return err
		}
	}
	return nil
}

func (img Image) IngestConfigFile(CVMFSRepo string) (err error) {
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}

	user := img.User
	pass, err := GetPassword()
	if err != nil {
		LogE(err).Warning("Unable to get the credential for downloading the configuration blog, trying anonymously")
		user = ""
		pass = ""
	}

	configUrl := fmt.Sprintf("%s://%s/v2/%s/blobs/%s",
		img.Scheme, img.Registry, img.Repository, manifest.Config.Digest)

	token, err := firstRequestForAuth(configUrl, user, pass)
	if err != nil {
		LogE(err).Warning("Unable to retrieve the token for downloading config file")
		return err
	}

	client := &http.Client{}
	req, err := http.NewRequest("GET", configUrl, nil)
	if err != nil {
		LogE(err).Warning("Unable to create a request for getting config file.")
		return err
	}
	req.Header.Set("Authorization", token)
	req.Header.Set("Accept", "application/vnd.docker.distribution.manifest.v2+json")

	resp, err := client.Do(req)
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		LogE(err).Warning("Error in reading the body from the configuration")
		return err
	}

	//write configuration to a temp file
	tmpFile, err := ioutil.TempFile("", "configFile")
	err = ioutil.WriteFile(tmpFile.Name(), []byte(body), 0644)
	tmpFile.Close()
	if err != nil {
		LogE(err).Error("Error in writing to the config file")
		return err
	}

	//generate config file path to ingest above temp dir.
	fname, err := generateConfigFileName(manifest.Config.Digest)
	if err != nil {
		LogE(err).Warning("Error in generating config file name")
		return err
	}

	imageID := strings.Split(manifest.Config.Digest, ":")[1]
	configFilePath := filepath.Join(rootPath, imageMetadataDir, imageID, fname)

	//Ingest config file
	err = IngestIntoCVMFS(CVMFSRepo, configFilePath, tmpFile.Name())
	os.RemoveAll(tmpFile.Name())
	if err != nil {
		return err
	}
	return nil
}

func (img Image) IngestImageManifest(CVMFSRepo string) (err error) {
	manifest, err := img.GetManifest()
	if err != nil {
		LogE(err).Warn("Error in getting the image manifest")
		return err
	}
	imageID := strings.Split(manifest.Config.Digest, ":")[1]

	symlinkPath := filepath.Join(rootPath, imageMetadataDir, imageID, "manifest.json")
	targetPath := filepath.Join(".metadata", img.Registry, img.Repository + img.GetReference(), "manifest.json")

	err = CreateSymlinkIntoCVMFS(CVMFSRepo, symlinkPath, targetPath)
	if err != nil {
		LogE(err).Error("Error in creating the symlink for manifest.json")
		return err
	}
	return nil
}

func (img Image) CreateLockFiles(CVMFSRepo, fpath string) (err error) {
	lockFilePath := filepath.Join("/cvmfs", CVMFSRepo, fpath)
	if _, err := os.Stat(lockFilePath); os.IsNotExist(err) {
		tmpFile, err := ioutil.TempFile("", "lock")
		tmpFile.Close()
		if err != nil {
			return err
		}
		err = IngestIntoCVMFS(CVMFSRepo, TrimCVMFSRepoPrefix(lockFilePath), tmpFile.Name())
		os.RemoveAll(tmpFile.Name())
		if err != nil {
			return err
		}
	}
	return nil
}

func (img Image) CreatePodmanImageStore(CVMFSRepo, subDirInsideRepo string) (err error) {
	createCatalogIntoDirs := []string{rootPath, filepath.Join(rootPath,rootfsDir), filepath.Join(rootPath,imageMetadataDir), filepath.Join(rootPath,layerMetadataDir)}
	for _, dir := range createCatalogIntoDirs {	
		err = CreateCatalogIntoDir(CVMFSRepo, dir)
		if err != nil {
			LogE(err).WithFields(log.Fields{
				"directory": dir}).Error(
				"Impossible to create subcatalog in the directory.")
		}
	}

	err = img.IngestRootfsIntoPodmanStore(CVMFSRepo, subDirInsideRepo)
	if err != nil {
		LogE(err).Error("Error ingesting rootfs into podman image store")
		return err
	}

	err = img.CreateLinkDir(CVMFSRepo, subDirInsideRepo)
	if err != nil {
		LogE(err).Error("Unable to create the link dir in podman store")
		return err
	}

	err = img.IngestConfigFile(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create config file in podman store")
		return err
	}

	err = img.IngestImageManifest(CVMFSRepo)
	if err != nil {
		LogE(err).Error("Unable to create manifest file in podman store")
		return err
	}

	imagelockpath := filepath.Join(rootPath, imageMetadataDir, "images.lock")
	err = img.CreateLockFiles(CVMFSRepo, imagelockpath)
	if err != nil {
		LogE(err).Error("Unable to create images lock file in podman store")
		return err
	}

	layerlockpath := filepath.Join(rootPath, layerMetadataDir, "layers.lock")
	err = img.CreateLockFiles(CVMFSRepo, layerlockpath)
	if err != nil {
		LogE(err).Error("Unable to create layers lock file in podman store")
		return err
	}

	return nil
}