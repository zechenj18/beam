package exec

import (
	"sync"
	"time"
)

type DataSample struct {
	PCollectionID string
	Timestamp     time.Time
	Element       []byte
}

type OutputSamples struct {
	elements    []*DataSample
	mu          sync.Mutex
	maxElements int
	numSamples  int
	sampleIndex int
}

func (o *OutputSamples) addSample(element *DataSample) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.numSamples < o.maxElements {
		o.numSamples++
		o.elements = append(o.elements, element)
	} else {
		o.elements[o.sampleIndex] = element
		o.sampleIndex = (o.sampleIndex + 1) % o.maxElements
	}
}

func (o *OutputSamples) GetSamples() []*DataSample {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.numSamples == 0 {
		return nil
	}
	samples := o.elements

	// Reset index and number of samples
	o.numSamples = 0
	o.sampleIndex = 0
	// Release memory since samples are only returned once based on best efforts
	o.elements = nil

	return samples
}

type DataSampler struct {
	sampleChannel chan *DataSample
	mu            sync.RWMutex
	samplesMap    sync.Map // Key: PCollectionID string, Value: *OutputSamples pointer
	shutdown      int32
}

func NewDataSampler() *DataSampler {
	return &DataSampler{
		sampleChannel: make(chan *DataSample, 10),
	}
}

func (d *DataSampler) Start() {
	for sample := range d.sampleChannel {
		go d.addSample(sample)
	}
}

func (d *DataSampler) addSample(sample *DataSample) {
	p, ok := d.samplesMap.Load(sample.PCollectionID)
	if !ok {
		p = &OutputSamples{maxElements: 10, numSamples: 0, sampleIndex: 0}
		d.samplesMap.Store(sample.PCollectionID, p)
	}
	outputSamples := p.(*OutputSamples)
	outputSamples.addSample(sample)
}

func (d *DataSampler) getSamples(pCollectionID string) []*DataSample {
	p, ok := d.samplesMap.Load(pCollectionID)
	if !ok {
		return nil
	}
	outputSamples := p.(*OutputSamples)
	return outputSamples.GetSamples()
}

func (d *DataSampler) GetAllSamples() map[string][]*DataSample {
	var elementsMap = make(map[string][]*DataSample)
	d.samplesMap.Range(func(key interface{}, value interface{}) bool {
		pid := key.(string)
		samples := d.getSamples(pid)
		if len(samples) > 0 {
			elementsMap[pid] = samples
		}
		return true
	})
	return elementsMap
}

func (d *DataSampler) GetSamplesForPCollections(pids []string) map[string][]*DataSample {
	var elementsMap = make(map[string][]*DataSample)
	for _, pid := range pids {
		samples := d.getSamples(pid)
		if len(samples) > 0 {
			elementsMap[pid] = samples
		}
	}
	return elementsMap
}

func (d *DataSampler) Stop() {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.shutdown++
	close(d.sampleChannel)
}

func (d *DataSampler) SendSample(pCollectionID string, element []byte, timestamp time.Time) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if d.shutdown == 0 {
		sample := DataSample{
			PCollectionID: pCollectionID,
			Element:       element,
			Timestamp:     timestamp,
		}
		d.sampleChannel <- &sample
	}
}
